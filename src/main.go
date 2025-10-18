// main.go
package main

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/influxdata/influxdb/v2/tsdb/engine/tsm1"
	"github.com/xitongsys/parquet-go-source/local"
	parquet "github.com/xitongsys/parquet-go/parquet"
	parquetWritter "github.com/xitongsys/parquet-go/writer"
)

// ---------- Config ----------
var (
	MaxRowsPerFile  = int64(5_000_000) // rotate after this many rows per measurement file (tune for your environment)
	WriterQueueLen  = 1000             // per-measurement queue to smooth bursts
	ParallelReaders = 8                // number of goroutines reading TSM files concurrently
)

// ---------- Type helpers for schema inference ----------
type FieldType int

const (
	TypeUnknown FieldType = iota
	TypeInt64
	TypeDouble
	TypeBool
	TypeString
)

func (t FieldType) String() string {
	switch t {
	case TypeInt64:
		return "INT64"
	case TypeDouble:
		return "DOUBLE"
	case TypeBool:
		return "BOOLEAN"
	default:
		return "UTF8"
	}
}

// choose a field type promotion rule: if mixed numeric & float -> DOUBLE, mixed anything else -> UTF8
func promote(a, b FieldType) FieldType {
	if a == b {
		return a
	}
	if a == TypeUnknown {
		return b
	}
	if b == TypeUnknown {
		return a
	}
	// int64 + double => double
	if (a == TypeInt64 && b == TypeDouble) || (a == TypeDouble && b == TypeInt64) {
		return TypeDouble
	}
	// anything else becomes string
	return TypeString
}

// ---------- Structures to hold schema info ----------

type MeasurementSchema struct {
	Measurement string
	// tag keys (strings)
	TagKeys map[string]struct{}
	// field keys and inferred type
	FieldKeys map[string]FieldType

	mu sync.Mutex
}

func NewMeasurementSchema(name string) *MeasurementSchema {
	return &MeasurementSchema{
		Measurement: name,
		TagKeys:     make(map[string]struct{}),
		FieldKeys:   make(map[string]FieldType),
	}
}

func (ms *MeasurementSchema) AddTag(k string) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.TagKeys[k] = struct{}{}
}

func (ms *MeasurementSchema) AddFieldType(k string, t FieldType) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	cur := ms.FieldKeys[k]
	ms.FieldKeys[k] = promote(cur, t)
}

// ---------- Global registry ----------

var (
	schemasMu sync.Mutex
	schemas   = make(map[string]*MeasurementSchema)
)

func getOrCreateSchema(m string) *MeasurementSchema {
	schemasMu.Lock()
	defer schemasMu.Unlock()
	if s, ok := schemas[m]; ok {
		return s
	}
	ns := NewMeasurementSchema(m)
	schemas[m] = ns
	return ns
}

// ---------- Helpers to parse series key ----------
var seriesKeySplit = regexp.MustCompile(`,`) // simple split; tag values can contain commas escaped in Influx, adjust if needed

func parseSeriesKey(k string) (measurement string, tags map[string]string) {
	tags = map[string]string{}
	parts := strings.Split(k, ",")
	if len(parts) == 0 {
		return k, tags
	}
	measurement = parts[0]
	for _, p := range parts[1:] {
		kv := strings.SplitN(p, "=", 2)
		if len(kv) == 2 {
			tags[kv[0]] = kv[1]
		}
	}
	return
}

// ---------- Infer type from an interface{} value ----------
func inferTypeFromValue(v interface{}) FieldType {
	switch v := v.(type) {
	case int, int8, int16, int32, int64:
		return TypeInt64
	case uint, uint8, uint16, uint32, uint64:
		return TypeInt64
	case float32, float64:
		return TypeDouble
	case bool:
		return TypeBool
	case string:
		return TypeString
	default:
		// fallback: try to reflect / json decode -> string
		_ = v
		return TypeString
	}
}

// ---------- PASS 1: schema discovery ----------
func discoverSchemaFromTSMFile(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	r, err := tsm1.NewTSMReader(file)
	if err != nil {
		return err
	}
	defer r.Close()

	iter := r.BlockIterator()
	for iter.Next() {
		keyByte := iter.PeekNext()
		if "" == string(keyByte) {
			// Empty should not go here .
		}
		serieKey, fieldKey := tsm1.SeriesAndFieldFromCompositeKey(keyByte)
		fieldString := string(fieldKey)
		measurement, tags := parseSeriesKey(string(serieKey))
		s := getOrCreateSchema(measurement)
		// add tag keys
		for tk := range tags {
			s.AddTag(tk)
		}
		// values contains points of different fields; sample them to infer field keys & types
		values, err := r.ReadAll(keyByte)
		if err != nil {
			// What to do here ?
		}

		for _, vv := range values {
			// The exact API for vv may vary by Influx version.
			// We expect vv.Field()/vv.Value()/vv.UnixNano() or similar.
			// Try to use methods; if not available adapt here.
			fieldName := fieldString
			var v interface{}
			// attempt to extract
			// many versions expose vv.Field() or vv.FieldKey()
			// try both via type assertions using reflection fallback
			type fielder interface {
				Field() string
				Value() interface{}
			}
			if f, ok := vv.(fielder); ok {
				fieldName = f.Field()
				v = f.Value()
			} else {
				// Fallback: try to parse key for field suffix after a separator
				// NOTE: if this fallback is not correct for your Influx version, adapt here.
				// For now, use a placeholder field name "value"
				fieldName = fieldString
				v = vv.Value()
			}
			ft := inferTypeFromValue(v)
			s.AddFieldType(fieldName, ft)
		}
	}
	if err := iter.Err(); err != nil {
		// log and continue
		log.Printf("discover: iterator error on %s: %v", path, err)
	}
	return nil
}

// ---------- Build Parquet schema (Parquet thrift/JSON schema string) ----------
func buildParquetSchema(ms *MeasurementSchema) string {
	// We'll create a Parquet message with:
	// required INT64 timestamp (millis), then optional tag columns (UTF8), then optional fields with inferred types.
	// JSON writer expects a "message" style schema.
	var b bytes.Buffer
	b.WriteString("message m {\n")
	b.WriteString("  required int64 timestamp; \n")
	// tags (optional UTF8)
	tagList := make([]string, 0, len(ms.TagKeys))
	for k := range ms.TagKeys {
		tagList = append(tagList, k)
	}
	sort.Strings(tagList)
	for _, k := range tagList {
		safe := parquetColName(k)
		b.WriteString(fmt.Sprintf("  optional binary %s (UTF8);\n", safe))
	}
	// fields
	fieldList := make([]string, 0, len(ms.FieldKeys))
	for k := range ms.FieldKeys {
		fieldList = append(fieldList, k)
	}
	sort.Strings(fieldList)
	for _, k := range fieldList {
		t := ms.FieldKeys[k]
		safe := parquetColName(k)
		switch t {
		case TypeInt64:
			b.WriteString(fmt.Sprintf("  optional int64 %s;\n", safe))
		case TypeDouble:
			b.WriteString(fmt.Sprintf("  optional double %s;\n", safe))
		case TypeBool:
			b.WriteString(fmt.Sprintf("  optional boolean %s;\n", safe))
		default:
			b.WriteString(fmt.Sprintf("  optional binary %s (UTF8);\n", safe))
		}
	}
	b.WriteString("}\n")
	return b.String()
}

// safe column name: replace non-word chars with underscore
var nonWord = regexp.MustCompile(`\W+`)

func parquetColName(k string) string {
	s := nonWord.ReplaceAllString(k, "_")
	if s == "" {
		s = "col"
	}
	// avoid leading digit
	if s[0] >= '0' && s[0] <= '9' {
		s = "c_" + s
	}
	return s
}

// ---------- Writer goroutine per measurement ----------
type writerMsg struct {
	row map[string]interface{}
}

type measurementWriter struct {
	measurement string
	schemaStr   string

	outDir string

	queue chan writerMsg

	// rotation
	maxRows int64
	curRows int64
	fileIdx int64

	// parquet objects
	pw   *parquetWritter.JSONWriter
	fw   *local.LocalFile
	lock sync.Mutex
}

func newMeasurementWriter(measurement, schemaStr, outDir string, maxRows int64) *measurementWriter {
	mw := &measurementWriter{
		measurement: measurement,
		schemaStr:   schemaStr,
		outDir:      outDir,
		queue:       make(chan writerMsg, WriterQueueLen),
		maxRows:     maxRows,
		curRows:     0,
		fileIdx:     0,
	}
	go mw.run()
	return mw
}

func (mw *measurementWriter) enqueue(row map[string]interface{}) {
	mw.queue <- writerMsg{row: row}
}

func (mw *measurementWriter) rotate() error {
	// close existing
	mw.lock.Lock()
	defer mw.lock.Unlock()

	if mw.pw != nil {
		if err := mw.pw.WriteStop(); err != nil {
			log.Printf("parquet WriteStop error: %v", err)
		}
		if mw.fw != nil {
			_ = mw.fw.Close()
		}
		mw.pw = nil
		mw.fw = nil
	}

	// increment fileIdx
	mw.fileIdx++
	fileName := fmt.Sprintf("%s_%06d.parquet", sanitizeFileName(mw.measurement), mw.fileIdx)
	full := filepath.Join(mw.outDir, fileName)

	fw := &local.LocalFile{}
	_, err := fw.Create(full)
	if err != nil {
		return err
	}
	pw, err := parquetWritter.NewJSONWriter(mw.schemaStr, fw, 4)
	if err != nil {
		_ = fw.Close()
		return err
	}
	// set compression
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	mw.fw = fw
	mw.pw = pw
	mw.curRows = 0
	log.Printf("Started new parquet for %s -> %s", mw.measurement, full)
	return nil
}

func sanitizeFileName(s string) string {
	return strings.ReplaceAll(s, string(os.PathSeparator), "_")
}

func (mw *measurementWriter) run() {
	// create first file
	if err := os.MkdirAll(mw.outDir, 0755); err != nil {
		log.Fatalf("mkdir output: %v", err)
	}
	if err := mw.rotate(); err != nil {
		log.Fatalf("failed to open parquet for %s: %v", mw.measurement, err)
	}
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case msg, ok := <-mw.queue:
			if !ok {
				// flush and close
				_ = mw.rotate() // ensures writer closed
				return
			}
			mw.lock.Lock()
			// write row as JSON map
			if mw.pw == nil {
				_ = mw.rotate()
			}
			if err := mw.pw.Write(msg.row); err != nil {
				log.Printf("parquet write error for %s: %v", mw.measurement, err)
			} else {
				mw.curRows++
				if mw.curRows >= mw.maxRows {
					// rotate
					if err := mw.rotate(); err != nil {
						log.Printf("rotate error %v", err)
					}
				}
			}
			mw.lock.Unlock()
		case <-ticker.C:
			// periodic flush not strictly necessary; JSONWriter writes directly
		}
	}
}

// ---------- Global writers registry ----------

var (
	writersMu sync.Mutex
	writers   = make(map[string]*measurementWriter)
)

func getWriterForMeasurement(measurement string, schemaStr string, outDir string) *measurementWriter {
	writersMu.Lock()
	defer writersMu.Unlock()
	if w, ok := writers[measurement]; ok {
		return w
	}
	w := newMeasurementWriter(measurement, schemaStr, outDir, int64(MaxRowsPerFile))
	writers[measurement] = w
	return w
}

func closeAllWriters() {
	writersMu.Lock()
	defer writersMu.Unlock()
	for _, w := range writers {
		close(w.queue)
	}
}

// ---------- PASS 2: stream values to writers ----------
func processTSMFileStream(path string, outDir string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	r, err := tsm1.NewTSMReader(f)
	if err != nil {
		return err
	}
	defer r.Close()

	iter := r.BlockIterator()
	for iter.Next() {
		keyByte := iter.PeekNext()
		serieKey, fieldKey := tsm1.SeriesAndFieldFromCompositeKey(keyByte)
		field := string(fieldKey)
		key := string(serieKey)
		measurement, tags := parseSeriesKey(key)
		ms := getOrCreateSchema(measurement)
		// build a column name list (order doesn't matter for JSON writer)
		// create schema string if not already created (we build per measurement once)
		// first-time schemaStr creation:
		schemasMu.Lock()
		// ensure deterministic column ordering
		schemaStr := buildParquetSchema(ms)
		schemasMu.Unlock()

		writer := getWriterForMeasurement(measurement, schemaStr, outDir)
		values, err := r.ReadAll(keyByte)
		if err != nil {
			return err
		}
		for _, vv := range values {
			// extract field, value, timestamp
			var fieldName string
			var val interface{}
			var unixNano int64

			// try interface extraction
			type vIface interface {
				Field() string
				Value() interface{}
				UnixNano() int64
			}
			if vf, ok := vv.(vIface); ok {
				fieldName = vf.Field()
				val = vf.Value()
				unixNano = vf.UnixNano()
			} else {
				// fallback: call available methods or functions
				// IMPORTANT: adapt here if your tsm value type differs
				fieldName = field
				// try to call Value() and UnixNano() via reflection is possible, but we assume Value method exists
				type v2 interface {
					Value() interface{}
					UnixNano() int64
				}
				if v2f, ok2 := vv.(v2); ok2 {
					val = v2f.Value()
					unixNano = v2f.UnixNano()
				} else {
					// as last fallback, skip
					println("WARN: Could not extract value of values")
					continue
				}
			}

			// Build row map: timestamp (millis), tag columns, field column
			row := make(map[string]interface{})
			row["timestamp"] = unixNano

			// tags: ensure presence of all tag keys even if nil is okay for writer
			ms.mu.Lock()
			for tk := range ms.TagKeys {
				col := parquetColName(tk)
				if tv, ok := tags[tk]; ok {
					row[col] = tv
				} else {
					row[col] = nil
				}
			}
			// fields: ensure all field keys present; others nil
			for fk := range ms.FieldKeys {
				col := parquetColName(fk)
				if fk == fieldName {
					// convert val to one of the supported types (int64, float64, bool, string)
					switch vv := val.(type) {
					case int:
						row[col] = int64(vv)
					case int8:
						row[col] = int64(vv)
					case int16:
						row[col] = int64(vv)
					case int32:
						row[col] = int64(vv)
					case int64:
						row[col] = vv
					case uint, uint8, uint16, uint32, uint64:
						row[col] = int64(reflectToInt64(vv))
					case float32:
						row[col] = float64(vv)
					case float64:
						row[col] = vv
					case bool:
						row[col] = vv
					case string:
						row[col] = vv
					default:
						// as fallback convert to string
						row[col] = fmt.Sprintf("%v", vv)
					}
				} else {
					// other fields for this timestamp: null
					row[col] = nil
				}
			}
			ms.mu.Unlock()

			// enqueue
			writer.enqueue(row)
		}
	}
	if err := iter.Err(); err != nil {
		log.Printf("stream: iterator err %v", err)
	}
	return nil
}

func reflectToInt64(v interface{}) int64 {
	switch vv := v.(type) {
	case uint:
		return int64(vv)
	case uint8:
		return int64(vv)
	case uint16:
		return int64(vv)
	case uint32:
		return int64(vv)
	case uint64:
		return int64(vv)
	default:
		return 0
	}
}

// ---------- Utility: collect all .tsm files under a dir ----------
func collectTSMFiles(root string) ([]string, error) {
	var out []string
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			// continue
			return nil
		}
		if info.IsDir() {
			return nil
		}
		if strings.HasSuffix(path, ".tsm") {
			out = append(out, path)
		}
		return nil
	})
	return out, err
}

// ---------- Main ----------
func usage() {
	fmt.Printf("Usage: %s <tsm_dir> <output_dir>\n", os.Args[0])
	fmt.Printf("Config: MaxRowsPerFile=%d, ParallelReaders=%d\n", MaxRowsPerFile, ParallelReaders)
}

func main() {
	if len(os.Args) < 3 {
		usage()
		os.Exit(1)
	}
	tsmDir := os.Args[1]
	outDir := os.Args[2]

	files, err := collectTSMFiles(tsmDir)
	if err != nil {
		log.Fatalf("collectTSMFiles: %v", err)
	}
	log.Printf("Found %d tsm files", len(files))

	// PASS 1: schema discovery
	log.Printf("PASS 1: schema discovery (this should be fast compared to full export)")
	fileCh := make(chan string, 1024)
	var wg sync.WaitGroup
	for i := 0; i < ParallelReaders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for p := range fileCh {
				if err := discoverSchemaFromTSMFile(p); err != nil {
					log.Printf("schema discover error %s: %v", p, err)
				}
			}
		}()
	}
	for _, f := range files {
		fileCh <- f
	}
	close(fileCh)
	wg.Wait()

	// After discovery, print summary and build per-measurement schema strings (and show counts)
	log.Printf("Schema discovery complete. Measurements: %d", len(schemas))
	for m, s := range schemas {
		log.Printf("Measurement=%s tags=%d fields=%d", m, len(s.TagKeys), len(s.FieldKeys))
	}

	// PASS 2: streaming export
	log.Printf("PASS 2: streaming export")
	fileCh2 := make(chan string, 1024)
	var wg2 sync.WaitGroup
	for i := 0; i < ParallelReaders; i++ {
		wg2.Add(1)
		go func() {
			defer wg2.Done()
			for p := range fileCh2 {
				if err := processTSMFileStream(p, outDir); err != nil {
					log.Printf("streaming error %s: %v", p, err)
				}
			}
		}()
	}
	for _, f := range files {
		fileCh2 <- f
	}
	close(fileCh2)
	wg2.Wait()

	// close and flush writers
	closeAllWriters()

	log.Printf("Done export.")
}

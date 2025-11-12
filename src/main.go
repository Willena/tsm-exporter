// main.go
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/schollz/progressbar/v3"

	"github.com/influxdata/influxdb/v2/tsdb/engine/tsm1"
)

////////////////////////////////////////////////////////////////////////////////
// Types and constants
////////////////////////////////////////////////////////////////////////////////

type FieldType int

const (
	FieldInt FieldType = iota
	FieldFloat
	FieldString
	FieldBool
)

func (ft FieldType) Suffix() string {
	switch ft {
	case FieldInt:
		return "_i64"
	case FieldFloat:
		return "_f64"
	case FieldString:
		return "_str"
	case FieldBool:
		return "_bool"
	default:
		return "_str"
	}
}

func sqlTypeForField(ft FieldType) string {
	switch ft {
	case FieldInt:
		return "INTEGER"
	case FieldFloat:
		return "REAL"
	case FieldString:
		return "TEXT"
	case FieldBool:
		return "INTEGER"
	default:
		return "TEXT"
	}
}

type MeasurementSchema struct {
	FieldTypes map[string]FieldType // field key -> chosen type
	TagKeys    map[string]struct{}  // tag key set for the measurement
}

type Schema struct {
	Measurements map[string]*MeasurementSchema
	TagKeys      map[string]struct{} // global union of tag keys
}

func NewSchema() *Schema {
	return &Schema{
		Measurements: make(map[string]*MeasurementSchema),
		TagKeys:      make(map[string]struct{}),
	}
}

type Checkpoint struct {
	DoneFiles   map[string]struct{} `json:"done_files"`
	ProcessedAt time.Time           `json:"processed_at"`
}

func NewCheckpoint() *Checkpoint {
	return &Checkpoint{
		DoneFiles: make(map[string]struct{}),
	}
}

type Row struct {
	Measurement string
	Timestamp   int64
	Tags        map[string]string
	Fields      map[string]interface{}
}

////////////////////////////////////////////////////////////////////////////////
// CLI flags
////////////////////////////////////////////////////////////////////////////////

var (
	flTSMDir       = flag.String("tsm-dir", "", "TSM directory to read (required)")
	flOut          = flag.String("out", "", "output sqlite file (required)")
	flCheckpoint   = flag.String("checkpoint", "checkpoint.json", "checkpoint file path")
	flWorkers      = flag.Int("workers", runtime.NumCPU(), "number of parallel readers")
	flBatch        = flag.Int("batch", 1000, "sqlite insert batch size")
	flMeasurements = flag.String("measurements", "", "comma separated measurements to include (optional)")
	flStart        = flag.String("start", "", "start time RFC3339 or unix-ns (optional)")
	flEnd          = flag.String("end", "", "end time RFC3339 or unix-ns (optional)")
	flResume       = flag.Bool("resume", false, "resume from checkpoint file if present")
)

////////////////////////////////////////////////////////////////////////////////
// main
////////////////////////////////////////////////////////////////////////////////

func main() {
	flag.Parse()
	if *flTSMDir == "" || *flOut == "" {
		flag.Usage()
		os.Exit(2)
	}

	ctx := context.Background()

	var includeMeasurements map[string]struct{}
	if *flMeasurements != "" {
		includeMeasurements = make(map[string]struct{})
		for _, m := range strings.Split(*flMeasurements, ",") {
			includeMeasurements[strings.TrimSpace(m)] = struct{}{}
		}
	}

	startTime, endTime, err := parseTimeRange(*flStart, *flEnd)
	if err != nil {
		log.Fatalf("invalid time range: %v", err)
	}

	log.Println("PASS 1: schema discovery")
	schema, err := discoverSchema(ctx, *flTSMDir, includeMeasurements, startTime, endTime, *flWorkers)
	if err != nil {
		log.Fatalf("schema discovery failed: %v", err)
	}
	log.Printf("discovered %d measurements, %d unique tag keys\n", len(schema.Measurements), len(schema.TagKeys))

	log.Println("prepare sqlite file:", *flOut)
	db, err := sql.Open("sqlite3", *flOut)
	if err != nil {
		log.Fatalf("open sqlite: %v", err)
	}
	defer db.Close()
	if err := prepareSQLite(db, schema); err != nil {
		log.Fatalf("prepare sqlite failed: %v", err)
	}

	cp := NewCheckpoint()
	if *flResume {
		if err := loadCheckpoint(*flCheckpoint, cp); err != nil {
			log.Printf("warning: failed to load checkpoint (%v); starting fresh", err)
		} else {
			log.Printf("resuming: %d files already done", len(cp.DoneFiles))
		}
	}

	log.Println("PASS 2: exporting data")
	if err := exportData(ctx, *flTSMDir, schema, db, cp, *flCheckpoint, includeMeasurements, startTime, endTime, *flWorkers, *flBatch); err != nil {
		log.Fatalf("export failed: %v", err)
	}
	log.Println("export complete")
}

////////////////////////////////////////////////////////////////////////////////
// Time parsing
////////////////////////////////////////////////////////////////////////////////

func parseTimeRange(startStr, endStr string) (start, end *time.Time, err error) {
	parse := func(s string) (*time.Time, error) {
		if s == "" {
			return nil, nil
		}
		// try RFC3339Nano
		if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
			return &t, nil
		}
		// try RFC3339
		if t, err := time.Parse(time.RFC3339, s); err == nil {
			return &t, nil
		}
		// try integer nanoseconds
		if i, err := strconv.ParseInt(s, 10, 64); err == nil {
			t := time.Unix(0, i)
			return &t, nil
		}
		return nil, fmt.Errorf("can't parse time %q", s)
	}
	if startStr != "" {
		s, e := parse(startStr)
		if e != nil {
			return nil, nil, e
		}
		start = s
	}
	if endStr != "" {
		s, e := parse(endStr)
		if e != nil {
			return nil, nil, e
		}
		end = s
	}
	return start, end, nil
}

////////////////////////////////////////////////////////////////////////////////
// Schema discovery (pass 1)
////////////////////////////////////////////////////////////////////////////////

func discoverSchema(ctx context.Context, tsmDir string, includeMeasurements map[string]struct{}, start, end *time.Time, workers int) (*Schema, error) {
	schema := NewSchema()

	// gather tsm files
	files := []string{}
	err := filepath.WalkDir(tsmDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if strings.HasSuffix(d.Name(), ".tsm") || strings.HasSuffix(d.Name(), ".tsm1") {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if len(files) == 0 {
		log.Println("warning: no tsm files found")
	}

	fileCh := make(chan string, len(files))
	log.Printf("Found %d tsm files \n", len(files))

	for _, f := range files {
		fileCh <- f
	}
	close(fileCh)

	errCh := make(chan error, workers)
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for f := range fileCh {
				select {
				case <-ctx.Done():
					return
				default:
				}
				if err := inspectTSMFileForSchema(f, schema, includeMeasurements, start, end); err != nil {
					errCh <- fmt.Errorf("%s: %w", f, err)
					return
				}
			}
		}()
	}

	wg.Wait()
	close(errCh)
	if err, ok := <-errCh; ok {
		return nil, err
	}
	return schema, nil
}
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

func inspectTSMFileForSchema(filename string, schema *Schema, includeMeasurements map[string]struct{}, start, end *time.Time) error {
	f, err := os.Open(filename)
	log.Printf("Inspecting %s \n", filename)
	if err != nil {
		return err
	}
	defer f.Close()

	reader, err := tsm1.NewTSMReader(f)
	if err != nil {
		return err
	}
	defer reader.Close()

	iter := reader.BlockIterator()
	for iter.Next() {
		rawKey := iter.PeekNext()
		serieKey, fieldKey := tsm1.SeriesAndFieldFromCompositeKey(rawKey)
		field := string(fieldKey)
		key := string(serieKey)
		measurement, tagMap := parseSeriesKey(key)

		if includeMeasurements != nil {
			if _, ok := includeMeasurements[measurement]; !ok {
				continue
			}
		}

		// read some values to infer type
		values, err := reader.ReadAll(rawKey)
		if err != nil {
			// skip this key on read error
			continue
		}

		// Filter time range by checking if any value is inside range
		insideRange := false
		var observedType FieldType = FieldString // fallback
		for _, v := range values {
			ts := v.UnixNano()
			if start != nil && time.Unix(0, ts).Before(*start) {
				continue
			}
			if end != nil && time.Unix(0, ts).After(*end) {
				continue
			}
			insideRange = true
			switch v.Value().(type) {
			case float64:
				observedType = FieldFloat
			case int64:
				observedType = FieldInt
			case bool:
				observedType = FieldBool
			case string:
				observedType = FieldString
			case []byte:
				observedType = FieldString
			default:
				observedType = FieldString
			}
			// pick the most specific type if multiple values exist (if int and float both seen -> float)
			// We'll scan all values but break once we see float as it's more general for numeric.
			if observedType == FieldFloat {
				break
			}
		}
		if !insideRange {
			continue
		}

		// update schema (thread-unsafe code can be called concurrently; protect with mutex?)
		// In this function we may run concurrently from discoverSchema; simple approach:
		// Acquire a package-level mutex? For simplicity here we update schema atomically via a mutex stored in the schema object.
		// We'll embed a global mutex to protect these writes (defined below).
		updateSchemaWithObservation(schema, measurement, field, observedType, tagMap)
	}
	// check iter.Err()
	if err := iter.Err(); err != nil {
		// some iterator errors aren't fatal; log and continue
		// but return error to caller so discovery can be stopped if necessary
		// we'll return nil to be tolerant
		// return err
	}
	return nil
}

// schema mutex to safely update schema in concurrent discovery
var schemaMu sync.Mutex

func updateSchemaWithObservation(schema *Schema, measurement, fieldKey string, ft FieldType, tagMap map[string]string) {
	schemaMu.Lock()
	defer schemaMu.Unlock()
	ms, ok := schema.Measurements[measurement]
	if !ok {
		ms = &MeasurementSchema{
			FieldTypes: make(map[string]FieldType),
			TagKeys:    make(map[string]struct{}),
		}
		schema.Measurements[measurement] = ms
	}
	// update field type using simple union rules: int+float -> float, else string dominates conflicts
	if fieldKey != "" {
		if existing, ok := ms.FieldTypes[fieldKey]; !ok {
			ms.FieldTypes[fieldKey] = ft
		} else {
			if existing != ft {
				// unify
				if (existing == FieldInt && ft == FieldFloat) || (existing == FieldFloat && ft == FieldInt) {
					ms.FieldTypes[fieldKey] = FieldFloat
				} else {
					// fallback to string for other conflicts
					ms.FieldTypes[fieldKey] = FieldString
				}
			}
		}
	}
	for k := range tagMap {
		ms.TagKeys[k] = struct{}{}
		schema.TagKeys[k] = struct{}{}
	}
}

////////////////////////////////////////////////////////////////////////////////
// SQLite schema preparation
////////////////////////////////////////////////////////////////////////////////

func prepareSQLite(db *sql.DB, schema *Schema) error {
	// derive global union of fields across measurements and unify types
	fieldUnion := map[string]FieldType{}
	for _, ms := range schema.Measurements {
		for fk, ft := range ms.FieldTypes {
			if ex, ok := fieldUnion[fk]; !ok {
				fieldUnion[fk] = ft
			} else if ex != ft {
				// unify int+float -> float else string
				if (ex == FieldInt && ft == FieldFloat) || (ex == FieldFloat && ft == FieldInt) {
					fieldUnion[fk] = FieldFloat
				} else {
					fieldUnion[fk] = FieldString
				}
			}
		}
	}

	// stable ordering for deterministic schema
	tagKeys := make([]string, 0, len(schema.TagKeys))
	for t := range schema.TagKeys {
		tagKeys = append(tagKeys, t)
	}
	sort.Strings(tagKeys)

	fieldKeys := make([]string, 0, len(fieldUnion))
	for f := range fieldUnion {
		fieldKeys = append(fieldKeys, f)
	}
	sort.Strings(fieldKeys)

	// build CREATE TABLE
	sb := strings.Builder{}
	sb.WriteString("PRAGMA journal_mode=WAL;\n")
	if _, err := db.Exec(sb.String()); err != nil {
		// continue; not fatal
	}
	sb.Reset()
	sb.WriteString("CREATE TABLE IF NOT EXISTS tsm_data (\n")
	sb.WriteString("  measurement TEXT NOT NULL,\n")
	sb.WriteString("  timestamp INTEGER NOT NULL,\n")
	for i, t := range tagKeys {
		col := fmt.Sprintf("tag_%s TEXT", sanitizeColName(t))
		sb.WriteString("  " + col)
		if i < len(tagKeys)-1 || len(fieldKeys) > 0 {
			sb.WriteString(",\n")
		} else {
			sb.WriteString("\n")
		}
	}
	for i, f := range fieldKeys {
		ft := fieldUnion[f]
		col := fmt.Sprintf("%s %s", sanitizeColName(f)+ft.Suffix(), sqlTypeForField(ft))
		sb.WriteString("  " + col)
		if i < len(fieldKeys)-1 {
			sb.WriteString(",\n")
		} else {
			sb.WriteString("\n")
		}
	}
	sb.WriteString(");")
	createSQL := sb.String()
	if _, err := db.Exec(createSQL); err != nil {
		return fmt.Errorf("create table: %w\nsql=%s", err, createSQL)
	}
	return nil
}

func sanitizeColName(s string) string {
	// replace non-alphanumerics with underscore, ensure doesn't start with digit
	out := make([]rune, 0, len(s))
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			out = append(out, r)
		} else {
			out = append(out, '_')
		}
	}
	if len(out) == 0 {
		return "c"
	}
	if out[0] >= '0' && out[0] <= '9' {
		return "c" + string(out)
	}
	return string(out)
}

////////////////////////////////////////////////////////////////////////////////
// Export data (pass 2)
////////////////////////////////////////////////////////////////////////////////

func exportData(ctx context.Context, tsmDir string, schema *Schema, db *sql.DB, cp *Checkpoint, checkpointPath string, includeMeasurements map[string]struct{}, start, end *time.Time, workers, batchSize int) error {
	// collect files (skip done files)
	files := []string{}
	err := filepath.WalkDir(tsmDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if strings.HasSuffix(d.Name(), ".tsm") || strings.HasSuffix(d.Name(), ".tsm1") {
			if cp != nil {
				if _, done := cp.DoneFiles[path]; done {
					return nil
				}
			}
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if len(files) == 0 {
		log.Println("no files to process (maybe all done)")
		return nil
	}

	// Build ordered lists for columns
	tagKeys := make([]string, 0, len(schema.TagKeys))
	for t := range schema.TagKeys {
		tagKeys = append(tagKeys, t)
	}
	sort.Strings(tagKeys)
	tagColNames := make([]string, len(tagKeys))
	for i, t := range tagKeys {
		tagColNames[i] = "tag_" + sanitizeColName(t)
	}

	// global field union & ordering
	fieldUnion := map[string]FieldType{}
	for _, ms := range schema.Measurements {
		for fk, ft := range ms.FieldTypes {
			if ex, ok := fieldUnion[fk]; !ok {
				fieldUnion[fk] = ft
			} else if ex != ft {
				if (ex == FieldInt && ft == FieldFloat) || (ex == FieldFloat && ft == FieldInt) {
					fieldUnion[fk] = FieldFloat
				} else {
					fieldUnion[fk] = FieldString
				}
			}
		}
	}
	fieldKeys := make([]string, 0, len(fieldUnion))
	for f := range fieldUnion {
		fieldKeys = append(fieldKeys, f)
	}
	sort.Strings(fieldKeys)
	fieldColNames := make([]string, len(fieldKeys))
	for i, f := range fieldKeys {
		fieldColNames[i] = sanitizeColName(f) + fieldUnion[f].Suffix()
	}

	// columns order
	cols := []string{"measurement", "timestamp"}
	cols = append(cols, tagColNames...)
	cols = append(cols, fieldColNames...)

	// prepare insert
	placeholders := make([]string, len(cols))
	for i := range placeholders {
		placeholders[i] = "?"
	}
	insertSQL := fmt.Sprintf("INSERT INTO tsm_data (%s) VALUES (%s)", strings.Join(cols, ","), strings.Join(placeholders, ","))
	stmt, err := db.Prepare(insertSQL)
	if err != nil {
		return fmt.Errorf("prepare insert: %w", err)
	}
	defer stmt.Close()

	// channels
	rowCh := make(chan Row, batchSize*workers*2)
	errCh := make(chan error, workers+2)
	var totalRows uint64

	// progress bar
	bar := progressbar.NewOptions(-1,
		progressbar.OptionSetDescription("rows"),
		progressbar.OptionSetWriter(os.Stdout),
		progressbar.OptionShowCount(),
		progressbar.OptionSpinnerType(14),
	)

	// writer goroutine
	var writerWG sync.WaitGroup
	writerWG.Add(1)
	go func() {
		defer writerWG.Done()
		batch := make([]Row, 0, batchSize)
		commit := func(rows []Row) error {
			tx, err := db.Begin()
			if err != nil {
				return err
			}
			for _, r := range rows {
				args := make([]interface{}, 0, len(cols))
				args = append(args, r.Measurement, r.Timestamp)
				// tags
				for _, tk := range tagKeys {
					if v, ok := r.Tags[tk]; ok && v != "" {
						args = append(args, v)
					} else {
						args = append(args, nil)
					}
				}
				// fields
				for _, fk := range fieldKeys {
					if v, ok := r.Fields[fk]; ok {
						args = append(args, v)
					} else {
						args = append(args, nil)
					}
				}
				if _, err := tx.Stmt(stmt).Exec(args...); err != nil {
					_ = tx.Rollback()
					return err
				}
				atomic.AddUint64(&totalRows, 1)
			}
			if err := tx.Commit(); err != nil {
				return err
			}
			for i := 0; i < len(rows); i++ {
				_ = bar.Add(1)
			}
			return nil
		}

		flushTicker := time.NewTicker(2 * time.Second)
		defer flushTicker.Stop()

		for {
			select {
			case r, ok := <-rowCh:
				if !ok {
					if len(batch) > 0 {
						if err := commit(batch); err != nil {
							errCh <- err
							return
						}
					}
					return
				}
				batch = append(batch, r)
				if len(batch) >= batchSize {
					if err := commit(batch); err != nil {
						errCh <- err
						return
					}
					batch = batch[:0]
				}
			case <-flushTicker.C:
				if len(batch) > 0 {
					if err := commit(batch); err != nil {
						errCh <- err
						return
					}
					batch = batch[:0]
				}
			}
		}
	}()

	// readers
	var readersWG sync.WaitGroup
	fileCh := make(chan string, len(files))
	for _, f := range files {
		fileCh <- f
	}
	close(fileCh)

	sem := make(chan struct{}, workers)
	for i := 0; i < workers; i++ {
		readersWG.Add(1)
		go func(workerID int) {
			defer readersWG.Done()
			for f := range fileCh {
				select {
				case sem <- struct{}{}:
				case <-ctx.Done():
					return
				}
				log.Printf("[worker %d] processing file: %s", workerID, f)
				if err := streamRowsFromFile(ctx, f, schema, rowCh, includeMeasurements, start, end); err != nil {
					errCh <- fmt.Errorf("file %s: %w", f, err)
					<-sem
					return
				}
				<-sem
				// mark file done & persist checkpoint
				cp.DoneFiles[f] = struct{}{}
				if err := saveCheckpoint(checkpointPath, cp); err != nil {
					log.Printf("warning: failed to save checkpoint: %v", err)
				}
			}
		}(i)
	}

	// wait for readers to finish, then close rowCh
	go func() {
		readersWG.Wait()
		close(rowCh)
	}()

	// wait for writer or error
	select {
	case err := <-errCh:
		return err
	default:
		// wait for writer finish
		writerWG.Wait()
	}
	log.Printf("exported rows: %d\n", atomic.LoadUint64(&totalRows))
	return nil
}

////////////////////////////////////////////////////////////////////////////////
// streamRowsFromFile: read values and emit Row
////////////////////////////////////////////////////////////////////////////////

func streamRowsFromFile(ctx context.Context, filename string, schema *Schema, rowCh chan<- Row, includeMeasurements map[string]struct{}, start, end *time.Time) error {
	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	reader, err := tsm1.NewTSMReader(f)
	if err != nil {
		return err
	}
	defer reader.Close()

	iter := reader.BlockIterator()
	for iter.Next() {
		rawKey := iter.PeekNext()
		serieKey, fieldKey := tsm1.SeriesAndFieldFromCompositeKey(rawKey)
		field := string(fieldKey)
		key := string(serieKey)
		measurement, tagMap := parseSeriesKey(key)

		values, err := reader.ReadAll(rawKey)
		if err != nil {
			// skip on read error
			continue
		}
		for _, v := range values {
			ts := v.UnixNano()
			if start != nil && time.Unix(0, ts).Before(*start) {
				continue
			}
			if end != nil && time.Unix(0, ts).After(*end) {
				continue
			}
			val := v.Value()
			// coerce []byte -> string
			switch vv := val.(type) {
			case []byte:
				val = string(vv)
			}
			row := Row{
				Measurement: measurement,
				Timestamp:   ts,
				Tags:        tagMap,
				Fields:      map[string]interface{}{field: val},
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case rowCh <- row:
			}
		}
	}
	if err := iter.Err(); err != nil {
		// log and continue
		// return err
	}
	return nil
}

////////////////////////////////////////////////////////////////////////////////
// Checkpoint utilities
////////////////////////////////////////////////////////////////////////////////

func loadCheckpoint(path string, cp *Checkpoint) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return json.NewDecoder(f).Decode(cp)
}

func saveCheckpoint(path string, cp *Checkpoint) error {
	cp.ProcessedAt = time.Now().UTC()
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(cp); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

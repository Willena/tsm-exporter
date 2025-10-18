# tsm exporter

A quick and dirty POC for a tool that manipulate and convert RAW influxdb TSM data files to export them into other format. 

For now the POC is using parquet. 

The TSM file reading is handled by the offical influxv2 go package.
The format of the TSM is also file partially described in the influxdb v2 repository. 
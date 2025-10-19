# tsm exporter

A quick and dirty POC for a tool that manipulate and convert RAW influxdb TSM data files to export them into other format. 

For now the POC is using parquet. 

The TSM file reading is handled by the offical influxv2 go package.
The format of the TSM is also file partially described in the influxdb v2 repository. 

# Build

Somehow when pulling the tsm package from influx we end up pull the flux libraries. 
It requires a "special" environnement to be compiled from the rust source. 

You must have rust, pkg-config, the special influx pkg-config wrapper


```
# First install GNU pkg-config.
# On Debian/Ubuntu
sudo apt-get install -y rust clang pkg-config

# Or on Mac OS X with Homebrew
brew install rust pkg-config

# Next, install the pkg-config wrapper utility
go install github.com/influxdata/pkg-config

```

Ensure the pgk-config wrapper is available in the PATH

```
which -a pkg-config
/home/user/go/bin/pkg-config
/usr/bin/pkg-config
```

Then you can buil the tool

```
go build ./src/main.go
```




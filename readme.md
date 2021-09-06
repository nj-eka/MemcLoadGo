# MemcLoadGo
*Go* implementation of **MemcLoad** task (see here https://github.com/nj-eka/MemcLoad)
with adding the following features:
- comprehensive logging;
- advanced error handling;
- resilience: no data loss in case of errors;
- monitoring of app statistics in runtime;
- resumability (based on durable buffering): application can be gracefully stopped so that the next time app is started, it will continue to work from the point where it was stopped.


###Depends:
  - OS: Linux
  - Service: memchache
### Install:
    > go get https://github.com/nj-eka/MemcLoadGo
### Usage:
    > ./memc.sh
    > ./MemcLoadGo --help
    Usage of ./MemcLoadGo:
    -adid string
        memc address for 'adid' device type
    -buffers int
        Buffers count per device type (default 2)
    -dques string
        Dqueue directory (default "data/dques/")
    -dry
        Execution mode without modification (default true)
    -dvid string
        memc address for 'dvid' device type
    -gaid string
        memc address for 'gaid' device type
    -idfa string
        memc address for 'idfa' device type
    -l string
        Logging level: panic (short) (default "info")
    -loaders int
        Max count of loaders (max number of open input files) (default 1)
    -log string
        Path to logging output file (empty = os.Stdout) (default "MemcLoadGo.log")
    -log_format string
        Logging format: text (default "text")
    -log_level string
        Logging level: panic (default "info")
    -p string
        Input files pattern (short) (default "./data/appsinstalled/*.tsv.gz")
    -parsers int
        Max count of parsers (default 16)
    -pattern string
        Input files pattern (default "./data/appsinstalled/*.tsv.gz")
    -resume
        Resume from previous sessions (default true)
    -retries int
        memcache max retries (default 3)
    -retry_timeout duration
        memcache retry timeout
    -segment int
        Items per dqueue segment (default 65536)
    -timeout duration
        memcache operation timeout
    -trace string
        Trace output file (tracing is on if LogLevel == trace) (default "MemcLoadGo.trace.out")
    -turbo
        Dqueue turbo mode
    -unkignore 
        Skip errors for unknown input device type (default true) 
    -v    Display processing statistics (os.Stdout) (short)
    -verbose
        Display processing statistics (os.Stdout)


## Example:
    > ./MemcLoadGo --pattern=data/appsinstalled/h100000_*.tsv.gz
    Time elapsed:  9s
    Loader stats (read): done - 7.113216476s
    293 KiB(lines:   41 KiB per sec)  90 MiB(bytes:   13 MiB per sec)  3(files)
    Parser stats (proto):
    adid:   73 KiB(items)  22 MiB(bytes in)  15 MiB(bytes out)
    dvid:   74 KiB(items)  22 MiB(bytes in)  15 MiB(bytes out)
    gaid:   74 KiB(items)  22 MiB(bytes in)  15 MiB(bytes out)
    idfa:   73 KiB(items)  22 MiB(bytes in)  15 MiB(bytes out)
    DBuffer stats (dque):
    adid_0: ->         done - 7.113396s / ->         done - 7.353915s;   36 KiB/36 KiB   (items)   7.3 MiB/7.3 MiB  (bytes)
    adid_1: ->         done - 7.113395s / ->         done - 7.360946s;   36 KiB/36 KiB   (items)   7.3 MiB/7.3 MiB  (bytes)
    dvid_0: ->         done - 7.113365s / ->         done - 7.142295s;   37 KiB/37 KiB   (items)   7.4 MiB/7.4 MiB  (bytes)
    dvid_1: ->         done - 7.113353s / ->         done - 7.125507s;   37 KiB/37 KiB   (items)   7.4 MiB/7.4 MiB  (bytes)
    gaid_0: ->         done - 7.113439s / ->         done - 7.376196s;   36 KiB/36 KiB   (items)   7.3 MiB/7.3 MiB  (bytes)
    gaid_1: ->         done - 7.113420s / ->         done - 7.359442s;   37 KiB/37 KiB   (items)   7.5 MiB/7.5 MiB  (bytes)
    idfa_0: ->         done - 7.113353s / ->         done - 7.443679s;   37 KiB/37 KiB   (items)   7.4 MiB/7.4 MiB  (bytes)
    idfa_1: ->         done - 7.113363s / ->         done - 7.437519s;   36 KiB/36 KiB   (items)   7.3 MiB/7.3 MiB  (bytes)
    Saver stats (memc):
    adid:   73 KiB(items)  15 MiB(bytes)
    dvid:   74 KiB(items)  15 MiB(bytes)
    gaid:   74 KiB(items)  15 MiB(bytes)
    idfa:   73 KiB(items)  15 MiB(bytes)
    Mem.usage stats:        Alloc = 29 MiB  TotalAlloc = 1.4 GiB    Sys = 139 MiB   Mallocs = 14 MiB        Frees = 14 MiB  GCSys = 7.7 MiB NumGC = 59

    > /usr/bin/time -v ./MemcLoadGo --pattern=data/appsinstalled/h100000_*.tsv.gz
        Command being timed: "./MemcLoadGo --pattern=data/appsinstalled/h100000_*.tsv.gz"
        User time (seconds): 8.55
        System time (seconds): 8.05
        Percent of CPU this job got: 192%
        Elapsed (wall clock) time (h:mm:ss or m:ss): 0:08.64
        Average shared text size (kbytes): 0
        Average unshared data size (kbytes): 0
        Average stack size (kbytes): 0
        Average total size (kbytes): 0
        Maximum resident set size (kbytes): 91780
        Average resident set size (kbytes): 0
        Major (requiring I/O) page faults: 0
        Minor (reclaiming a frame) page faults: 15297
        Voluntary context switches: 37347
        Involuntary context switches: 272790
        Swaps: 0
        File system inputs: 0
        File system outputs: 161224
        Socket messages sent: 0
        Socket messages received: 0
        Signals delivered: 0
        Page size (bytes): 4096
        Exit status: 0

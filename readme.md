# MemcLoadGo
*Go* implementation of **MemcLoad** task (see here https://github.com/nj-eka/MemcLoad)
with adding the following features:
- comprehensive logging;
- advanced error handling;
- resilience: no data loss (++/-) in case of errors;
- monitoring statistics in runtime;
- resumability: application can be gracefully stopped so that next launch time it will continue to processing data from last stopped point.


### Depends:
  - OS: Linux
  - Service: memchache
### Install:
    > git clone https://github.com/nj-eka/MemcLoadGo
    > cd MemcLoadGo
    > go mod tidy
    > go build .
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
            Run mode without modification (default true)
        -dvid string
            memc address for 'dvid' device type
        -gaid string
            memc address for 'gaid' device type
        -idfa string
            memc address for 'idfa' device type
        -l string
            Logging level: panic fatal error warn info debug trace (short) (default "info")
        -loaders int
            Max count of loaders (max number of open input files) (default 1)
        -log string
            Path to log output file; empty = os.Stdout (default "MemcLoadGo.log")
        -log_format string
            Logging format: text json (default "text")
        -log_level string
            Logging level: panic fatal error warn info debug trace (default "info")
        -p string
            Input files pattern (short) (default "./data/appsinstalled/*.tsv.gz")
        -parsers int
            Max count of parsers (default 16)
        -pattern string
            Input files pattern (default "./data/appsinstalled/*.tsv.gz")
        -resume
            Resumable (default true)
        -retries int
            memcache max retries (default 3)
        -retry_timeout duration
            memcache retry timeout
        -segment int
            Items per dqueue segment (default 65536)
        -timeout duration
            memcache operation timeout
        -trace string
            Trace file; tracing is on if LogLevel = trace; empty = os.Stderr (default "MemcLoadGo.trace.out")
        -turbo
            Dqueue turbo mode (default true)
        -ignore_unknown 
            Skip errors for unknown input device type (default true)
        -v    
            Display processing statistics (os.Stdout) (short)
        -verbose    
            Display processing statistics (os.Stdout)

## Example:
    > /usr/bin/time -v ./MemcLoadGo --pattern=data/appsinstalled/h100000_*.tsv.gz
    Time elapsed:  10s
    Mem.usage stats:        Alloc = 34 MiB  TotalAlloc = 1.3 GiB    Sys = 139 MiB   Mallocs = 14 MiB        Frees = 13 MiB  GCSys = 7.6 MiB NumGC = 41
    Loader stats (read): done - 8.433725094    293 KiB(lines:   35 KiB/s)  90 MiB(bytes:   11 MiB/s)  3(files)
    Parser stats (proto):
        adid:   73 KiB(items)  22 MiB(bytes in)  15 MiB(bytes out)
        dvid:   74 KiB(items)  22 MiB(bytes in)  15 MiB(bytes out)
        gaid:   74 KiB(items)  22 MiB(bytes in)  15 MiB(bytes out)
        idfa:   73 KiB(items)  22 MiB(bytes in)  15 MiB(bytes out)
    DBuffer stats (dque):
        adid_0: ->         done - 8.433739s / ->         done - 8.948796s;   36 KiB/36 KiB   (items)   7.3 MiB/7.3 MiB  (bytes)
        adid_1: ->         done - 8.433720s / ->         done - 8.993608s;   37 KiB/37 KiB   (items)   7.4 MiB/7.4 MiB  (bytes)
        dvid_0: ->         done - 8.433747s / ->         done - 9.007456s;   37 KiB/37 KiB   (items)   7.4 MiB/7.4 MiB  (bytes)
        dvid_1: ->         done - 8.433762s / ->         done - 8.989257s;   37 KiB/37 KiB   (items)   7.4 MiB/7.4 MiB  (bytes)
        gaid_0: ->         done - 8.433775s / ->         done - 8.937590s;   37 KiB/37 KiB   (items)   7.4 MiB/7.4 MiB  (bytes)
        gaid_1: ->         done - 8.433797s / ->         done - 8.805843s;   37 KiB/37 KiB   (items)   7.4 MiB/7.4 MiB  (bytes)
        idfa_0: ->         done - 8.433783s / ->         done - 9.145435s;   36 KiB/36 KiB   (items)   7.3 MiB/7.3 MiB  (bytes)
        idfa_1: ->         done - 8.433824s / ->         done - 9.804922s;   37 KiB/37 KiB   (items)   7.4 MiB/7.4 MiB  (bytes)
    Saver stats (memc):
        adid:   73 KiB(items)  15 MiB(bytes)
        dvid:   74 KiB(items)  15 MiB(bytes)
        gaid:   74 KiB(items)  15 MiB(bytes)
        idfa:   73 KiB(items)  15 MiB(bytes)
            Command being timed: "./MemcLoadGo --pattern=data/appsinstalled/h100000_*.tsv.gz"
            User time (seconds): 10.09
            System time (seconds): 9.66
            Percent of CPU this job got: 190%
            Elapsed (wall clock) time (h:mm:ss or m:ss): 0:10.35
            Average shared text size (kbytes): 0
            Average unshared data size (kbytes): 0
            Average stack size (kbytes): 0
            Average total size (kbytes): 0
            Maximum resident set size (kbytes): 90360
            Average resident set size (kbytes): 0
            Major (requiring I/O) page faults: 0
            Minor (reclaiming a frame) page faults: 17737
            Voluntary context switches: 60005
            Involuntary context switches: 266830
            Swaps: 0
            File system inputs: 0
            File system outputs: 161232
            Socket messages sent: 0
            Socket messages received: 0
            Signals delivered: 0
            Page size (bytes): 4096
            Exit status: 0

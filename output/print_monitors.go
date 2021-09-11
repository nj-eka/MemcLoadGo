package output

import (
	"bufio"
	"fmt"
	cu "github.com/nj-eka/MemcLoadGo/ctxutils"
	erf "github.com/nj-eka/MemcLoadGo/errsflow"
	"github.com/nj-eka/MemcLoadGo/fh"
	"github.com/nj-eka/MemcLoadGo/logging"
	"github.com/nj-eka/MemcLoadGo/reg"
	"github.com/nj-eka/MemcLoadGo/workflow"
	"math"
	"os"
	"runtime"
	"sort"
	"time"
)

const (
	upLeft     = "\n\033[H\033[2J"
	colorReset = "\033[0m"

	colorRed    = "\033[31m"
	colorGreen  = "\033[32m"
	colorYellow = "\033[33m"
	colorBlue   = "\033[34m"
	colorPurple = "\033[35m"
	colorCyan   = "\033[36m"
	//colorWhite  = "\033[37m"
)

func PrintProcessMonitors(
	startTime time.Time,
	statProducers ...workflow.StatProducer,
) {
	ctx := cu.BuildContext(nil, cu.SetContextOperation("print_monitors"))
	bufOut := bufio.NewWriter(os.Stdout)
	bout := func(s string) {
		if _, err := bufOut.WriteString(s); err != nil {
			logging.LogError(ctx, fmt.Sprintf("bufio write string [%s] failed: %w", s, err))
		}
	}
	var (
		loaderStats   *workflow.LoaderStats
		dtParserStats *workflow.DeviceTypeParserStats
		dtDBufStats   *workflow.DeviceTypeDBufferStats
		dtSaverStats  *workflow.DeviceTypeSaverStats
		errsStats     erf.ErrorStats
	)
	for _, statProducer := range statProducers {
		switch st := statProducer.Stats().(type) {
		case *workflow.LoaderStats:
			loaderStats = st
		case *workflow.DeviceTypeParserStats:
			dtParserStats = st
		case *workflow.DeviceTypeDBufferStats:
			dtDBufStats = st
		case *workflow.DeviceTypeSaverStats:
			dtSaverStats = st
		case erf.ErrorStats:
			errsStats = st
		}
	}

	bout(fmt.Sprintln(upLeft))
	bout(fmt.Sprintln("Time elapsed: ", time.Since(startTime).Round(time.Second)))

	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	bout(fmt.Sprint(colorCyan, "Mem.usage stats:"))
	bout(fmt.Sprintf("\tAlloc = %v", fh.BytesToHuman(ms.Alloc)))           // Alloc is bytes of allocated heap objects. HeapAlloc is bytes of allocated heap objects.
	bout(fmt.Sprintf("\tTotalAlloc = %v", fh.BytesToHuman(ms.TotalAlloc))) // TotalAlloc is cumulative bytes allocated for heap objects.
	bout(fmt.Sprintf("\tSys = %v", fh.BytesToHuman(ms.Sys)))               // Sys is the total bytes of memory obtained from the OS.
	bout(fmt.Sprintf("\tMallocs = %v", fh.BytesToHuman(ms.Mallocs)))       // Mallocs is the cumulative count of heap objects allocated.
	bout(fmt.Sprintf("\tFrees = %v", fh.BytesToHuman(ms.Frees)))           // Frees is the cumulative count of heap objects freed.
	bout(fmt.Sprintf("\tGCSys = %v", fh.BytesToHuman(ms.GCSys)))           // GCSys is bytes of memory in garbage collection metadata.
	bout(fmt.Sprintf("\tNumGC = %v\n", ms.NumGC))
	bout(fmt.Sprint(colorReset))

	// Intermediate stage statistics are inconsistent
	// since in fact it is calculated at different points in time
	// However, final result will be correct in any case
	if loaderStats != nil {
		since := time.Since(loaderStats.StartTime)
		status := "in progress"
		if !loaderStats.FinishTime.IsZero() {
			since = loaderStats.FinishTime.Sub(loaderStats.StartTime)
			status = "done"
		}
		bout(fmt.Sprint(colorBlue))
		bout(fmt.Sprintf("Loader stats (read): %s - %v\n", status, since.Seconds()))
		lines, bytes := loaderStats.ItemsCounter.GetCountScore()
		bout(fmt.Sprintf("%8s(lines: %8s/s)", fh.BytesToHuman(uint64(lines)), fh.BytesToHuman(uint64(math.Ceil(float64(lines)/since.Seconds())))))
		bout(fmt.Sprintf("%8s(bytes: %8s/s)", fh.BytesToHuman(uint64(bytes)), fh.BytesToHuman(uint64(math.Ceil(float64(bytes)/since.Seconds())))))
		bout(fmt.Sprintf("%3d(files)\n", loaderStats.FilesCounter.GetScore()))
	}
	if dtParserStats != nil {
		bout(fmt.Sprint(colorGreen, "Parser stats (proto):\n"))
		for _, deviceType := range workflow.DTParserStats(dtParserStats.DTStats).SortByDeviceType() {
			bout(fmt.Sprintf("%8s: %8s(items)", deviceType, fh.BytesToHuman(uint64(dtParserStats.DTStats[deviceType].ItemsCounter().GetScore()))))
			bout(fmt.Sprintf("%8s(bytes in)", fh.BytesToHuman(uint64(dtParserStats.DTStats[deviceType].InputBytesCounter().GetScore()))))
			bout(fmt.Sprintf("%8s(bytes out)\n", fh.BytesToHuman(uint64(dtParserStats.DTStats[deviceType].OutputBytesCounter().GetScore()))))
		}
	}

	if dtDBufStats != nil {
		bout(fmt.Sprint(colorYellow, "DBuffer stats (dque):\n"))
		for _, deviceType := range workflow.DTBufferStats(dtDBufStats.DTInputStats).SortByDeviceType() {
			upSince, upStatus := time.Since(dtDBufStats.StartTime), "in progress"
			if !dtDBufStats.DTInputStats[deviceType].EndTime.IsZero() {
				upSince, upStatus = dtDBufStats.DTInputStats[deviceType].EndTime.Sub(dtDBufStats.StartTime), "done"
			}
			outSince, outStatus := upSince, "in progress"
			if !dtDBufStats.DTOutputStats[deviceType].EndTime.IsZero() {
				outSince, outStatus = dtDBufStats.DTOutputStats[deviceType].EndTime.Sub(dtDBufStats.StartTime), "done"
			}
			inputItems, inputBytes := dtDBufStats.DTInputStats[deviceType].ItemsCounter.GetCountScore()
			outputItems, outputBytes := dtDBufStats.DTOutputStats[deviceType].ItemsCounter.GetCountScore()
			bout(fmt.Sprintf(
				"%8s: -> %12s - %6fs / -> %12s - %6fs; %8s/%-8s (items)\t",
				deviceType,
				upStatus,
				upSince.Seconds(),
				outStatus,
				outSince.Seconds(),
				fh.BytesToHuman(uint64(inputItems)),
				fh.BytesToHuman(uint64(outputItems)),
			))
			bout(fmt.Sprintf(
				"%8s/%-8s (bytes)\n",
				fh.BytesToHuman(uint64(inputBytes)),
				fh.BytesToHuman(uint64(outputBytes)),
			))
		}
	}

	if dtSaverStats != nil {
		bout(fmt.Sprint(colorPurple, "Saver stats (memc):\n"))
		for _, deviceType := range workflow.DTSaverStats(dtSaverStats.DTStats).SortByDeviceType() {
			items, bytes := dtSaverStats.DTStats[deviceType].ItemsCounter.GetCountScore()
			bout(fmt.Sprintf(
				"%8s: %8s(items)",
				deviceType,
				fh.BytesToHuman(uint64(items)),
			))
			bout(fmt.Sprintf(
				"%8s(bytes)\n",
				fh.BytesToHuman(uint64(bytes)),
			))
		}
	}

	if errsStats != nil {
		cp := errsStats.GetCounterPairs()
		if len(cp) > 0 {
			bout(fmt.Sprintln(colorRed, "Errors:"))
			sort.Sort(reg.CounterPairsByKey(cp))
			for _, cp := range cp {
				esk := cp.Key.(erf.ErrStatKey)
				bout(fmt.Sprintf(" *%-8s: %-48s # %4d - %s\n", esk.Severity, esk.Operations, cp.Count, esk.Kind))
			}
		}
	}

	bout(fmt.Sprint(colorReset))

	if err := bufOut.Flush(); err != nil {
		logging.LogError(ctx, fmt.Errorf("bufio flush failed: %w", err))
	}
}

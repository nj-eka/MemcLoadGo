package output

import (
	"fmt"
	erf "github.com/nj-eka/MemcLoadGo/errsflow"
	"github.com/nj-eka/MemcLoadGo/fh"
	"github.com/nj-eka/MemcLoadGo/regs"
	"github.com/nj-eka/MemcLoadGo/workflow"
	"math"
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
	verbose bool,
	loaderStats *workflow.LoaderStats,
	dtParserStats *workflow.DeviceTypeParserStats,
	dtDBufStats *workflow.DeviceTypeDBufferStats,
	dtSaverStats *workflow.DeviceTypeSaverStats,
	errs *erf.ErrorsStat,
) {
	if verbose {
		// there may be an inconsistent statistic for intermediate stage (since it is shown at different points in time)
		// here it's considered acceptable, since the final result will be correct in any case
		fmt.Println(upLeft)
		fmt.Println("Time elapsed: ", time.Since(startTime).Round(time.Second))
		since := time.Since(loaderStats.StartTime)
		status := "in progress"
		if !loaderStats.FinishTime.IsZero() {
			since = loaderStats.FinishTime.Sub(loaderStats.StartTime)
			status = "done"
		}
		fmt.Print(colorBlue)
		fmt.Printf("Loader stats (read): %s - %v\n", status, since.Seconds())
		lines, bytes := loaderStats.ItemsCounter.GetCountScore()
		fmt.Printf("%8s(lines: %8s/s)", fh.BytesToHuman(uint64(lines)), fh.BytesToHuman(uint64(math.Ceil(float64(lines)/since.Seconds()))))
		fmt.Printf("%8s(bytes: %8s/s)", fh.BytesToHuman(uint64(bytes)), fh.BytesToHuman(uint64(math.Ceil(float64(bytes)/since.Seconds()))))
		fmt.Printf("%3d(files)\n", loaderStats.FilesCounter.GetScore())

		fmt.Print(colorGreen, "Parser stats (proto):\n")
		for _, deviceType := range workflow.DTParserStats(dtParserStats.DTStats).SortByDeviceType() {
			fmt.Printf("%8s: %8s(items)", deviceType, fh.BytesToHuman(uint64(dtParserStats.DTStats[deviceType].ItemsCounter().GetScore())))
			fmt.Printf("%8s(bytes in)", fh.BytesToHuman(uint64(dtParserStats.DTStats[deviceType].InputBytesCounter().GetScore())))
			fmt.Printf("%8s(bytes out)\n", fh.BytesToHuman(uint64(dtParserStats.DTStats[deviceType].OutputBytesCounter().GetScore())))
		}

		if dtDBufStats != nil {
			fmt.Print(colorYellow, "DBuffer stats (dque):\n")
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
				fmt.Printf(
					"%8s: -> %12s - %6fs / -> %12s - %6fs; %8s/%-8s (items)\t",
					deviceType,
					upStatus,
					upSince.Seconds(),
					outStatus,
					outSince.Seconds(),
					fh.BytesToHuman(uint64(inputItems)),
					fh.BytesToHuman(uint64(outputItems)),
				)
				fmt.Printf(
					"%8s/%-8s (bytes)\n",
					fh.BytesToHuman(uint64(inputBytes)),
					fh.BytesToHuman(uint64(outputBytes)),
				)
			}
		}

		fmt.Print(colorPurple, "Saver stats (memc):\n")
		for _, deviceType := range workflow.DTSaverStats(dtSaverStats.DTStats).SortByDeviceType() {
			items, bytes := dtSaverStats.DTStats[deviceType].ItemsCounter.GetCountScore()
			fmt.Printf(
				"%8s: %8s(items)",
				deviceType,
				fh.BytesToHuman(uint64(items)),
			)
			fmt.Printf(
				"%8s(bytes)\n",
				fh.BytesToHuman(uint64(bytes)),
			)
		}
		//fmt.Print(colorReset)
		PrintMemUsage()
	}
	PrintErrorsStat(errs.Stats)
}

func PrintMemUsage() {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	// For info on each, see: https://golang.org/pkg/runtime/#MemStats
	fmt.Print(colorCyan, "Mem.usage stats:")
	fmt.Printf("\tAlloc = %v", fh.BytesToHuman(ms.Alloc))           // Alloc is bytes of allocated heap objects. HeapAlloc is bytes of allocated heap objects.
	fmt.Printf("\tTotalAlloc = %v", fh.BytesToHuman(ms.TotalAlloc)) // TotalAlloc is cumulative bytes allocated for heap objects.
	fmt.Printf("\tSys = %v", fh.BytesToHuman(ms.Sys))               // Sys is the total bytes of memory obtained from the OS.
	fmt.Printf("\tMallocs = %v", fh.BytesToHuman(ms.Mallocs))       // Mallocs is the cumulative count of heap objects allocated.
	fmt.Printf("\tFrees = %v", fh.BytesToHuman(ms.Frees))           // Frees is the cumulative count of heap objects freed.
	fmt.Printf("\tGCSys = %v", fh.BytesToHuman(ms.GCSys))           // GCSys is bytes of memory in garbage collection metadata.
	fmt.Printf("\tNumGC = %v\n", ms.NumGC)
	fmt.Print(colorReset)
}

func PrintErrorsStat(es regs.Decounter) {
	cp := es.GetCounterPairs()
	if len(cp) == 0 {
		return
	}
	fmt.Println(colorRed, "Errors:")
	sort.Sort(regs.CounterPairsByKey(cp))
	for _, cp := range cp {
		esk := cp.Key.(erf.ErrStatKey)
		fmt.Printf(" *%-8s: %-48s # %4d - %s\n", esk.Severity, esk.Operations, cp.Count, esk.Kind)
	}
	fmt.Print(colorReset)
}

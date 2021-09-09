package workflow

import (
	"bufio"
	"compress/gzip"
	"context"
	"fmt"
	cu "github.com/nj-eka/MemcLoadGo/ctxutils"
	"github.com/nj-eka/MemcLoadGo/errs"
	"github.com/nj-eka/MemcLoadGo/fh"
	"github.com/nj-eka/MemcLoadGo/logging"
	"github.com/nj-eka/MemcLoadGo/regs"
	"io"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type LoaderStats struct {
	StartTime, FinishTime      time.Time
	FilesCounter, ItemsCounter regs.Counter
}

type Loader interface {
	Run(ctx context.Context) <-chan struct{}
	ResCh() <-chan string
	ErrCh() <-chan errs.Error
	Stats() interface{}
}

type loader struct {
	resCh      chan string
	errCh      chan errs.Error
	stats      LoaderStats

	filePaths []string
	maxWorkers int
	usr        *user.User
	dry        bool
}

func NewLoader(ctx context.Context, filePaths []string, maxWorkers int, usr *user.User, dry bool, statsOn bool) Loader {
	ctx = cu.BuildContext(ctx, cu.SetContextOperation("1.0.loader_init"))
	return &loader{
		filePaths: filePaths,
		maxWorkers: maxWorkers,
		usr:        usr,
		dry:        dry,
		resCh:      make(chan string, maxWorkers),
		errCh:      make(chan errs.Error, maxWorkers*8),
		stats: LoaderStats{
			FilesCounter: regs.NewCounter(0, statsOn),
			ItemsCounter: regs.NewCounter(0, statsOn),
		},
	}
}

func (r *loader) Run(ctx context.Context) <-chan struct{} {
	ctx = cu.BuildContext(ctx, cu.SetContextOperation("1.loader"))
	done := make(chan struct{})

	r.stats.StartTime = time.Now()
	source := make(chan string)

	go func(ctx context.Context) {
		ctx = cu.BuildContext(ctx, cu.AddContextOperation("iter_sources"))
		defer OnExit(ctx, r.errCh, "iter_sources",
			func() {
				close(source)
			})
		for _, filePath := range r.filePaths {
			if fullFilePath, err := fh.ResolvePath(filePath, r.usr); err != nil {
				r.errCh <- errs.E(ctx, errs.KindInvalidValue, fmt.Errorf("resolving file [%s] failed: %w", filePath, err))
			} else {
				select {
				case <-ctx.Done():
					return
				case source <- fullFilePath:
				}
			}
		}
	}(ctx)

	go func(ctx context.Context) {
		ctx = cu.BuildContext(ctx, cu.AddContextOperation("workers"))
		wg := sync.WaitGroup{}
		wp := make(chan struct{}, r.maxWorkers)

		defer OnExit(ctx, r.errCh, "workers",
			func() {
				// wait for graceful closing all open files
				// with saving unprocessed data in current session
				// for next start from current position
				wg.Wait()
				r.stats.FinishTime = time.Now()
				close(wp)
				close(r.resCh)
				close(r.errCh)
				close(done)
			})
		for {
			select {
			case <-ctx.Done():
				return
			case filePath, more := <-source:
				if !more {
					return
				}
				select {
				case <-ctx.Done():
					return
				case wp <- struct{}{}:
					wg.Add(1)
					go processFile(ctx, &wg, wp, filePath, r.resCh, r.errCh, &r.stats, r.dry)
				}
			}
		}
	}(ctx)

	return done
}

func processFile(ctx context.Context, wg *sync.WaitGroup, wp <-chan struct{}, filePath string, resCh chan<- string, errCh chan<- errs.Error, sts *LoaderStats, dry bool) {
	ctx = cu.BuildContext(ctx, cu.AddContextOperation("processFile"))
	defer OnExit(ctx, errCh, fmt.Sprintf("reading file [%s]", filePath), func() {
		sts.FilesCounter.Add(1)
		<-wp
		wg.Done()
	})
	if file, err := os.Open(filePath); err == nil {
		logging.Msg(ctx).Debug("> open ", filePath)
		defer func() {
			err := file.Close()
			logging.Msg(ctx).Debug("> close ", filePath, " with err:", err)
			if !dry {
				path, name := filepath.Split(filePath)
				if err := os.Rename(filePath, filepath.Join(path, "."+name)); err != nil {
					errCh <- errs.E(ctx, errs.KindIO, fmt.Errorf("renaming file [%s] failed: %w", filePath, err))
				}
			}
		}()
		if fileInfo, err := file.Stat(); err == nil {
			if gz, err := gzip.NewReader(file); err == nil {
				logging.Msg(ctx).Debug("> gzip open ", filePath)
				defer func() {
					err := gz.Close()
					logging.Msg(ctx).Debug("< gzip read ", filePath, "  closed with err:", err)
				}()
				bufSize := 64 * 1024 * 1024 // todo: make ram dependent or add as config option
				if fileInfo.Size() < int64(bufSize) {
					bufSize = int(fileInfo.Size())
				}
				reader := bufio.NewReaderSize(gz, bufSize)
				for {
					line, err := reader.ReadString('\n') // delim <- gz.Header.OS
					if err != nil && err != io.EOF {
						lines, bytes := sts.ItemsCounter.GetCountScore()
						errCh <- errs.E(ctx, errs.KindGzip, fmt.Errorf(
							"< gzip read [%s] failed with %d/%d read and err: %w",
							filePath,
							lines, bytes,
							err,
						))
						return
					}
					line = strings.TrimRight(line, "\n")
					if len(line) > 0 { // skip empty string
						select {
						case <-ctx.Done():
							logging.Msg(ctx).Infof("processing file [%s] is stopped at the line: <%s>", filePath, line)
							if !dry {
								path, name := filepath.Split(filePath)
								target := filepath.Join(path, fmt.Sprintf("_%s_%s.gz", time.Now().Format("20060102150405"), strings.TrimRight(name, filepath.Ext(name))))
								if writer, err := os.Create(target); err == nil {
									logging.Msg(ctx).Debug("> create discard file ", target)
									defer func() {
										err := writer.Close()
										logging.Msg(ctx).Debug("< discard file [", target, "] closed with err: ", err)
									}()
									archiver := gzip.NewWriter(writer)
									archiver.Name = gz.Name
									defer func() {
										err := archiver.Close()
										logging.Msg(ctx).Debug("< gzip discard file [", target, "] closed with err: ", err)
									}()
									// save current / last line not sent yet for next stage of processing
									_, err := archiver.Write([]byte(line))
									if err == nil {
										// save rest of current open file for next session
										_, err = io.Copy(archiver, reader)
									}
									if err != nil {
										errCh <- errs.E(ctx, errs.KindGzip, fmt.Errorf("< gzip write to discard file [%s] failed: %w", target, err))
									}
								} else {
									errCh <- errs.E(ctx, errs.KindOpenFile, fmt.Errorf("< create discard file [%s] failed: %w", target, err))
								}
							}
							return
						case resCh <- line:
							sts.ItemsCounter.Add(len(line))
						}
					}
					if err == io.EOF {
						logging.Msg(ctx).Infof("read input file [%s] - ok: (%d/%d)", filePath, sts.ItemsCounter.GetCount(), sts.ItemsCounter.GetScore())
						return
					}
				}
			} else {
				errCh <- errs.E(ctx, errs.KindGzip, fmt.Errorf("< gzip open [%s] failed: %w", filePath, err))
			}
		} else {
			errCh <- errs.E(ctx, errs.KindIO, fmt.Errorf("< stat [%s] failed: %w", filePath, err))
		}
	} else {
		errCh <- errs.E(ctx, errs.KindOpenFile, fmt.Errorf("< open [%s] failed: %w", filePath, err))
	}
}

func (r *loader) ResCh() <-chan string {
	return r.resCh
}

func (r *loader) ErrCh() <-chan errs.Error {
	return r.errCh
}

func (r *loader) Stats() interface{} {
	return &r.stats
}

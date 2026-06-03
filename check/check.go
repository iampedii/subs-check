package check

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/beck-8/subs-check/check/platform"
	"github.com/beck-8/subs-check/config"
	proxyutils "github.com/beck-8/subs-check/proxy"
	"github.com/juju/ratelimit"
	"github.com/metacubex/mihomo/adapter"
	"github.com/metacubex/mihomo/constant"
)

// Result stores a node check result.
type Result struct {
	Proxy      map[string]any
	Openai     *platform.OpenAIResult
	Youtube    string
	Netflix    *platform.NetflixResult
	Google     bool
	Cloudflare bool
	Disney     bool
	Gemini     string
	TikTok     string
	Claude     string
	Spotify    string
	IP         string
	IPRisk     string
	Country    string
	Speed      int // KB/s, 0 means untested or failed the speed test.
}

// aliveResult is the intermediate result after the alive check passes.
type aliveResult struct {
	Proxy map[string]any
}

// ProxyChecker handles proxy checks.
// Per-stage counts live on package-level atomics (Progress / Available /
// MediaDone / FilterPassed / SpeedDone / SpeedOk) so both the CLI progress
// UI and the web admin API can read them without plumbing through a pointer.
type ProxyChecker struct {
	results    []Result
	proxyCount int
	progress   int32 // alive-stage done count; shared with showProgress
	available  int32 // alive-stage pass count;  shared with showProgress
}

var Progress atomic.Uint32
var Available atomic.Uint32
var ProxyCount atomic.Uint32
var TotalBytes atomic.Uint64
var Phase atomic.Uint32 // 0=idle, 1=pipeline running

// Pipeline-wide live counters. These are 0 when idle and reflect
// how many items have cleared each stage during an active run.
// AliveDone/AliveOk mirror Progress/Available for backward-compat
// with the single-phase progress UI.
var (
	MediaDone    atomic.Uint32 // checkMedia completions (pass-through, never drops)
	FilterPassed atomic.Uint32 // items that matched the filter and entered speed/collector
	SpeedDone    atomic.Uint32 // checkSpeed completions (pass + fail)
	SpeedOk      atomic.Uint32 // checkSpeed passes (also equals collector input when hasSpeedTest)
)

// PhaseResult stores the final result for one stage.
type PhaseResult struct {
	Available uint32 `json:"available"`
	Total     uint32 `json:"total"`
}

// PhaseResults stores final results for each stage for the frontend history view.
var PhaseResults [4]atomic.Pointer[PhaseResult] // indexes 1-3 map to the three stages.

func SavePhaseResult(phase int, available, total uint32) {
	if phase >= 1 && phase <= 3 {
		PhaseResults[phase].Store(&PhaseResult{Available: available, Total: total})
	}
}

func GetPhaseResult(phase int) *PhaseResult {
	if phase >= 1 && phase <= 3 {
		return PhaseResults[phase].Load()
	}
	return nil
}

func ResetPhaseResults() {
	for i := 1; i <= 3; i++ {
		PhaseResults[i].Store(nil)
	}
}

var progressPaused atomic.Bool
var progressRendered atomic.Bool

// activeCancelMu guards activeCancel.
var activeCancelMu sync.Mutex

// activeCancel cancels the currently running phase. nil when idle or
// between phases; the per-phase dispatcher installs and clears it.
var activeCancel context.CancelFunc

// RequestCancel aborts the currently running check phase, if any.
// Safe to call from any goroutine; no-op when idle.
// Phases installed after this call are unaffected (per-phase scope,
// matching the pre-context ForceClose-reset-between-phases behaviour).
//
// Deliberately silent: emitting a log here would land between the
// progress renderer's rows and get overwritten by the next frame's
// cursor-up escape. run() logs the cancellation after pauseProgress
// has parked the renderer.
func RequestCancel() {
	activeCancelMu.Lock()
	defer activeCancelMu.Unlock()
	if activeCancel != nil {
		activeCancel()
	}
}

// installPhaseCancel registers cancel as the active phase canceller
// and returns a cleanup closure that cancels and clears it.
// Call the returned closure from a defer in the phase function.
func installPhaseCancel(cancel context.CancelFunc) func() {
	activeCancelMu.Lock()
	activeCancel = cancel
	activeCancelMu.Unlock()
	return func() {
		activeCancelMu.Lock()
		activeCancel = nil
		activeCancelMu.Unlock()
		cancel()
	}
}

var Bucket *ratelimit.Bucket

var probePacer = &globalProbePacer{}

type globalProbePacer struct {
	mu   sync.Mutex
	next time.Time
}

func resetProbePacer() {
	probePacer.mu.Lock()
	probePacer.next = time.Time{}
	probePacer.mu.Unlock()
}

func reserveProbeSlot(interval time.Duration, now time.Time) time.Time {
	probePacer.mu.Lock()
	defer probePacer.mu.Unlock()

	if probePacer.next.Before(now) {
		probePacer.next = now
	}
	slot := probePacer.next
	probePacer.next = slot.Add(interval)
	return slot
}

func waitForProbeSlot(ctx context.Context) error {
	intervalMS := config.GlobalConfig.ProbeIntervalMS
	if intervalMS <= 0 {
		return nil
	}

	now := time.Now()
	slot := reserveProbeSlot(time.Duration(intervalMS)*time.Millisecond, now)
	delay := slot.Sub(now)
	if delay <= 0 {
		return nil
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// effectiveConcurrency calculates the effective concurrency for a stage.
func effectiveConcurrency(phaseConcurrency, fallback, itemCount int) int {
	c := phaseConcurrency
	if c <= 0 {
		c = fallback
	}
	if itemCount < c {
		c = itemCount
	}
	if c < 1 {
		c = 1
	}
	return c
}

// Check runs the proxy check pipeline.
func Check() ([]Result, error) {
	proxyutils.ResetRenameCounter()

	ProxyCount.Store(0)
	Available.Store(0)
	Progress.Store(0)
	Phase.Store(0)

	TotalBytes.Store(0)

	// Prepend keep-days history nodes.
	var proxies []map[string]any
	if len(config.GlobalProxies) > 0 {
		slog.Info(fmt.Sprintf("Added history nodes to the check queue: %d", len(config.GlobalProxies)))
		proxies = append(proxies, config.GlobalProxies...)
	}
	tmp, err := proxyutils.GetProxies()
	if err != nil {
		return nil, fmt.Errorf("failed to get nodes: %w", err)
	}
	proxies = append(proxies, tmp...)
	slog.Info(fmt.Sprintf("Fetched nodes: %d", len(proxies)))

	// Reset global nodes.
	config.GlobalProxies = make([]map[string]any, 0)

	proxies = proxyutils.DeduplicateProxies(proxies)
	slog.Info(fmt.Sprintf("Nodes after deduplication: %d", len(proxies)))
	if limit := config.GlobalConfig.MaxProbesPerRun; limit > 0 && len(proxies) > limit {
		before := len(proxies)
		proxies = limitProxiesForRun(proxies, limit, config.GlobalConfig.ShuffleTestOrder)
		slog.Warn("Probe cap enabled; limiting node checks", "before", before, "after", len(proxies))
	}

	checker := &ProxyChecker{
		results: make([]Result, 0),
	}
	return checker.run(proxies)
}

// run drives the 4-stage pipeline: dispatch → alive → media+filter → speed → collect.
// Stages run concurrently, connected by channels. SuccessLimit cancels the whole
// pipeline as soon as the collector has gathered N passing items; in-flight work
// is drained and un-dispatched items are discarded.
func (pc *ProxyChecker) run(proxies []map[string]any) ([]Result, error) {
	resetProbePacer()

	if config.GlobalConfig.TotalSpeedLimit != 0 {
		Bucket = ratelimit.NewBucketWithRate(float64(config.GlobalConfig.TotalSpeedLimit*1024*1024), int64(config.GlobalConfig.TotalSpeedLimit*1024*1024/10))
	} else {
		Bucket = ratelimit.NewBucketWithRate(float64(math.MaxInt64), int64(math.MaxInt64))
	}

	slog.Info("Starting node checks")
	slog.Info("Current parameters", "timeout", config.GlobalConfig.Timeout, "enable-speedtest", config.GlobalConfig.SpeedTestUrl != "", "min-speed", config.GlobalConfig.MinSpeed, "download-timeout", config.GlobalConfig.DownloadTimeout, "download-mb", config.GlobalConfig.DownloadMB, "total-speed-limit", config.GlobalConfig.TotalSpeedLimit)

	ResetPhaseResults()

	done := make(chan bool)
	if config.GlobalConfig.PrintProgress {
		go pc.showProgress(done)
	}

	// Capture the speed-test URL once at pipeline start so the current run
	// stays consistent even if the user edits config mid-check. Otherwise
	// flipping the URL to empty mid-run would cause every in-flight speed
	// request to fail (no host) and silently drop nearly all results.
	speedTestURL := config.GlobalConfig.SpeedTestUrl
	hasSpeedTest := speedTestURL != ""
	total := len(proxies)

	aliveConcurrency := effectiveConcurrency(config.GlobalConfig.Concurrent, config.GlobalConfig.Concurrent, total)
	mediaConcurrency := effectiveConcurrency(config.GlobalConfig.MediaConcurrent, config.GlobalConfig.Concurrent, total)
	speedConcurrency := effectiveConcurrency(config.GlobalConfig.SpeedConcurrent, config.GlobalConfig.Concurrent, total)
	slog.Info(fmt.Sprintf("Starting pipeline: input=%d, concurrency(alive/media/speed)=%d/%d/%d", total, aliveConcurrency, mediaConcurrency, speedConcurrency))

	// showProgress keeps reading pc.progress / pc.available / pc.proxyCount;
	// the alive stage owns these counters throughout the pipeline run.
	pc.resetPhaseCounters(total)
	Phase.Store(1)
	resumeProgress()

	// Compile filter patterns once; media workers re-use the slice.
	patterns := CompileFilterPatterns()
	if len(patterns) > 0 {
		slog.Info(fmt.Sprintf("Applying node filters: %d regexes", len(patterns)))
	}

	// Whole-pipeline cancellation: collector pulls the trigger on SuccessLimit,
	// RequestCancel pulls it on external SIGHUP / HTTP force-close.
	ctx, cancel := context.WithCancel(context.Background())
	defer installPhaseCancel(cancel)()

	// Channels sized to each stage's concurrency to keep buffering bounded.
	aliveIn := make(chan aliveTask, aliveConcurrency)
	mediaIn := make(chan mediaEntry, mediaConcurrency)
	speedIn := make(chan pipelineItem, speedConcurrency)
	collectIn := make(chan pipelineItem, speedConcurrency)

	if config.GlobalConfig.ShuffleTestOrder {
		slog.Info("Shuffled node check order; output still preserves subscription order")
	}

	// Dispatcher
	go pipelineDispatch(ctx, proxies, aliveIn)

	// Alive workers
	aliveWg := pc.startAliveWorkers(ctx, aliveConcurrency, aliveIn, mediaIn)
	go func() { aliveWg.Wait(); close(mediaIn) }()

	// Media workers (filter runs inline on each passing item)
	mediaWg := pc.startMediaWorkers(ctx, mediaConcurrency, mediaIn, speedIn, collectIn, hasSpeedTest, patterns)
	go func() {
		mediaWg.Wait()
		close(speedIn)
		if !hasSpeedTest {
			close(collectIn)
		}
	}()

	// Speed workers (optional)
	if hasSpeedTest {
		speedWg := pc.startSpeedWorkers(ctx, speedConcurrency, speedIn, collectIn, speedTestURL)
		go func() { speedWg.Wait(); close(collectIn) }()
	}

	// Collector: place items in pre-allocated slots to preserve subscription order.
	// The SuccessLimit hit notice is *not* logged here: emitting slog output
	// mid-render interleaves with the progress writer and breaks cursor-up
	// positioning. We remember whether we tripped the limit and log it after
	// pauseProgress has parked the renderer.
	out := make([]*Result, total)
	var finalPassed int32
	limitHit := false
	for item := range collectIn {
		r := item.r
		out[item.idx] = &r
		finalPassed++
		if config.GlobalConfig.SuccessLimit > 0 && finalPassed >= config.GlobalConfig.SuccessLimit && !limitHit {
			limitHit = true
			cancel()
		}
	}

	pauseProgress()

	if limitHit {
		slog.Warn(fmt.Sprintf("Success limit reached: %d; pipeline stopped", config.GlobalConfig.SuccessLimit))
	} else if ctx.Err() != nil {
		// External cancel (RequestCancel via SIGHUP / HTTP force-close).
		// Logged here rather than in RequestCancel because emitting it
		// while the progress renderer is still drawing would let the
		// next frame's cursor-up escape overwrite the warn line.
		slog.Warn("Cancel signal received; pipeline stopped")
	}

	// Snapshot per-stage results. Totals cascade: alive counts against input,
	// media counts against alive, speed counts against filter-passed.
	aliveOk := Available.Load()
	mediaDone := MediaDone.Load()
	filterPassed := FilterPassed.Load()
	SavePhaseResult(1, aliveOk, uint32(total))
	SavePhaseResult(2, mediaDone, aliveOk)
	if hasSpeedTest {
		SavePhaseResult(3, SpeedOk.Load(), filterPassed)
	}

	// Flatten in subscription order, dropping empty slots.
	pc.results = make([]Result, 0, finalPassed)
	for _, r := range out {
		if r != nil {
			pc.results = append(pc.results, *r)
		}
	}

	if config.GlobalConfig.PrintProgress {
		done <- true
	}
	Phase.Store(0)

	slog.Info(fmt.Sprintf("Alive nodes: %d", aliveOk))
	if len(patterns) > 0 {
		slog.Info(fmt.Sprintf("Nodes before filtering: %d, after filtering: %d", mediaDone, filterPassed))
	} else if hasSpeedTest {
		slog.Info(fmt.Sprintf("Media stage passed nodes: %d", filterPassed))
	}
	slog.Info(fmt.Sprintf("Usable nodes: %d", len(pc.results)))
	slog.Info(fmt.Sprintf("Total traffic used by tests: %.3fGB", float64(TotalBytes.Load())/1024/1024/1024))

	pc.checkSubscriptionSuccessRate(proxies)

	return pc.results, nil
}

// ======= Pipeline types =======

// aliveTask is an input proxy plus its original index (for order preservation).
type aliveTask struct {
	idx   int
	proxy map[string]any
}

// mediaEntry carries an alive-pass proxy into the media stage.
type mediaEntry struct {
	idx int
	a   aliveResult
}

// pipelineItem flows through filter, speed test, and the collector.
type pipelineItem struct {
	idx int
	r   Result
}

// ======= Pipeline stages =======
//
// The pipeline runs alive / media+filter / speed concurrently, connected
// by channels. An idx field rides along each item so the collector can
// restore original subscription order before emitting the final slice.
//
// Cancellation: a single context.Context covers the entire pipeline.
// SuccessLimit causes the collector to cancel it once N passes have been
// gathered; goroutines then drain their inputs and exit.

// pipelineDispatch feeds proxies into the alive stage. By default it dispatches
// in subscription order; with shuffle-test-order on, it dispatches in a random
// permutation so same-airport / multi-protocol nodes don't cluster in one test
// window. Either way each task carries its original index, so the collector
// restores subscription order on output regardless of dispatch sequence.
// Closes out on exit and honours ctx cancellation.
func pipelineDispatch(ctx context.Context, proxies []map[string]any, out chan<- aliveTask) {
	defer close(out)

	order := make([]int, len(proxies))
	for i := range order {
		order[i] = i
	}
	if config.GlobalConfig.ShuffleTestOrder {
		rand.Shuffle(len(order), func(a, b int) { order[a], order[b] = order[b], order[a] })
	}

	for _, i := range order {
		select {
		case <-ctx.Done():
			return
		case out <- aliveTask{idx: i, proxy: proxies[i]}:
		}
	}
}

func limitProxiesForRun(proxies []map[string]any, limit int, shuffle bool) []map[string]any {
	if limit <= 0 || len(proxies) <= limit {
		return proxies
	}
	if !shuffle {
		return proxies[:limit]
	}

	order := make([]int, len(proxies))
	for i := range order {
		order[i] = i
	}
	rand.Shuffle(len(order), func(a, b int) { order[a], order[b] = order[b], order[a] })

	limited := make([]map[string]any, limit)
	for i := 0; i < limit; i++ {
		limited[i] = proxies[order[i]]
	}
	return limited
}

// startAliveWorkers spawns n alive-check workers.
// Cancellation policy (middle stage): when cancel fires, workers stop
// pulling new items from their input queue *and* allow in-flight items
// to be dropped at the send boundary via a ctx-aware select. This
// prevents items queued at the time of cancel from triggering wasted
// work in the downstream media / speed stages.
// Items already classified as "passed speed" never get dropped — see
// startSpeedWorkers for the asymmetric policy at the last boundary.
func (pc *ProxyChecker) startAliveWorkers(ctx context.Context, n int, in <-chan aliveTask, out chan<- mediaEntry) *sync.WaitGroup {
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for t := range in {
				if ctx.Err() != nil {
					return
				}
				r := pc.checkAlive(t.proxy)
				pc.incrementProgress()
				if r == nil {
					continue
				}
				pc.incrementAvailable()
				select {
				case <-ctx.Done():
					return
				case out <- mediaEntry{idx: t.idx, a: *r}:
				}
			}
		}()
	}
	return &wg
}

// startMediaWorkers spawns n media-check workers.
// checkMedia always produces a Result; the worker applies the filter inline
// and forwards passing items to speedOut (hasSpeed) or collectOut (!hasSpeed).
// Cancellation policy:
//   - hasSpeed: middle stage, ctx-aware select on speedOut (drops on cancel
//     race so cancel-dropped items don't waste a ~10s speed test)
//   - !hasSpeed: last stage, unconditional send to collectOut so items
//     classified as passing the filter are never dropped at the final hop
func (pc *ProxyChecker) startMediaWorkers(
	ctx context.Context,
	n int,
	in <-chan mediaEntry,
	speedOut, collectOut chan<- pipelineItem,
	hasSpeed bool,
	patterns []*regexp.Regexp,
) *sync.WaitGroup {
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for entry := range in {
				if ctx.Err() != nil {
					return
				}
				res := pc.checkMedia(entry.a)
				MediaDone.Add(1)
				if res == nil || !MatchesFilter(*res, patterns) {
					continue
				}
				FilterPassed.Add(1)
				if hasSpeed {
					select {
					case <-ctx.Done():
						return
					case speedOut <- pipelineItem{idx: entry.idx, r: *res}:
					}
				} else {
					// last stage — collector always reads collectIn, so the
					// unconditional send never blocks; guarantees every
					// filter-passed item ends up in the output.
					collectOut <- pipelineItem{idx: entry.idx, r: *res}
				}
			}
		}()
	}
	return &wg
}

// startSpeedWorkers spawns n speed-test workers.
// Last stage before the collector: items that pass min-speed are sent
// unconditionally so an item classified as "good" is never dropped at
// the final hop, even if cancel fires between SpeedOk.Add and the send.
// ctx.Err is only checked at the top of the loop to avoid starting a
// fresh ~10s speed test once we've already tripped SuccessLimit.
//
// speedTestURL is passed through (captured at pipeline start) so the
// run stays self-consistent even if the user edits SpeedTestUrl in
// the config file mid-check.
func (pc *ProxyChecker) startSpeedWorkers(ctx context.Context, n int, in <-chan pipelineItem, out chan<- pipelineItem, speedTestURL string) *sync.WaitGroup {
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for item := range in {
				if ctx.Err() != nil {
					return
				}
				updated := pc.checkSpeed(item.r, speedTestURL)
				SpeedDone.Add(1)
				if updated == nil {
					continue
				}
				SpeedOk.Add(1)
				out <- pipelineItem{idx: item.idx, r: *updated}
			}
		}()
	}
	return &wg
}

// checkAlive checks whether a single proxy is alive.
func (pc *ProxyChecker) checkAlive(proxy map[string]any) *aliveResult {
	if os.Getenv("SUB_CHECK_SKIP") != "" {
		return &aliveResult{Proxy: proxy}
	}

	httpClient := CreateClient(proxy)
	if httpClient == nil {
		return nil
	}
	defer httpClient.Close()

	alive, err := platform.CheckAlive(httpClient.Client)
	if err != nil || !alive {
		return nil
	}

	return &aliveResult{Proxy: proxy}
}

// checkSpeed runs a speed test on an existing Result.
// Nodes that pass min-speed get r.Speed set and are returned; failed nodes return nil.
// proxy["name"] is not modified.
// speedTestURL is a snapshot frozen by the caller when the pipeline starts, so a
// config hot reload that clears the URL does not break every speed-test request
// in the current run with a no-host error.
func (pc *ProxyChecker) checkSpeed(r Result, speedTestURL string) *Result {
	if os.Getenv("SUB_CHECK_SKIP") != "" {
		r.Speed = 0
		return &r
	}

	httpClient := CreateClient(r.Proxy)
	if httpClient == nil {
		return nil
	}
	defer httpClient.Close()

	speed, _, err := platform.CheckSpeed(httpClient.Client, Bucket, httpClient.BytesRead, speedTestURL)
	if err != nil || speed < config.GlobalConfig.MinSpeed {
		return nil
	}

	r.Speed = speed
	return &r
}

// checkMedia runs media checks and any required country lookup.
// It does not discard nodes or modify proxy["name"]; results are written to
// Result's structured fields.
// Counter updates are owned by the caller (media pipeline worker).
func (pc *ProxyChecker) checkMedia(a aliveResult) *Result {
	res := &Result{Proxy: a.Proxy}

	if os.Getenv("SUB_CHECK_SKIP") != "" {
		return res
	}

	httpClient := CreateClient(a.Proxy)
	if httpClient == nil {
		return res
	}
	defer httpClient.Close()

	if config.GlobalConfig.MediaCheck {
		mediaTimeout := config.GlobalConfig.MediaCheckTimeout
		if mediaTimeout <= 0 {
			mediaTimeout = 10
		}
		mediaClient := &http.Client{
			Transport: httpClient.Client.Transport,
			Timeout:   time.Duration(mediaTimeout) * time.Second,
		}

		// Check all platforms in parallel.
		var mediaWg sync.WaitGroup
		for _, plat := range config.GlobalConfig.Platforms {
			switch plat {
			case "openai":
				mediaWg.Add(1)
				go func() {
					defer mediaWg.Done()
					res.Openai = platform.CheckOpenAI(mediaClient)
				}()
			case "youtube":
				mediaWg.Add(1)
				go func() {
					defer mediaWg.Done()
					if region, _ := platform.CheckYoutube(mediaClient); region != "" {
						res.Youtube = region
					}
				}()
			case "netflix":
				mediaWg.Add(1)
				go func() {
					defer mediaWg.Done()
					nf, _ := platform.CheckNetflix(mediaClient)
					res.Netflix = nf
				}()
			case "disney":
				mediaWg.Add(1)
				go func() {
					defer mediaWg.Done()
					if ok, _ := platform.CheckDisney(mediaClient); ok {
						res.Disney = true
					}
				}()
			case "gemini":
				mediaWg.Add(1)
				go func() {
					defer mediaWg.Done()
					if region, _ := platform.CheckGemini(mediaClient); region != "" {
						res.Gemini = region
					}
				}()
			case "claude":
				mediaWg.Add(1)
				go func() {
					defer mediaWg.Done()
					if region, _ := platform.CheckClaude(mediaClient); region != "" {
						res.Claude = region
					}
				}()
			case "spotify":
				mediaWg.Add(1)
				go func() {
					defer mediaWg.Done()
					if region, _ := platform.CheckSpotify(mediaClient); region != "" {
						res.Spotify = region
					}
				}()
			case "iprisk":
				mediaWg.Add(1)
				go func() {
					defer mediaWg.Done()
					country, ip := proxyutils.GetProxyCountry(mediaClient)
					if ip == "" {
						return
					}
					res.IP = ip
					res.Country = country
					risk, err := platform.CheckIPRisk(mediaClient, ip)
					if err == nil {
						res.IPRisk = risk
					} else {
						slog.Debug(fmt.Sprintf("IP risk lookup failed: %v", err))
					}
				}()
			case "tiktok":
				mediaWg.Add(1)
				go func() {
					defer mediaWg.Done()
					if region, _ := platform.CheckTikTok(mediaClient); region != "" {
						res.TikTok = region
					}
				}()
			}
		}
		mediaWg.Wait()
	}

	// If iprisk did not populate Country but naming needs country data,
	// explicitly query the country once.
	if res.Country == "" && (config.GlobalConfig.RenameNode || config.GlobalConfig.NodeNameTemplate != "") {
		country, _ := proxyutils.GetProxyCountry(httpClient.Client)
		res.Country = country
	}

	return res
}

// pauseProgress pauses progress rendering and writes a newline so later logs do not share the same line.
func pauseProgress() {
	progressPaused.Store(true)
	time.Sleep(150 * time.Millisecond) // Wait for the progress goroutine to stop writing.
	if progressRendered.Load() {
		fmt.Println()                 // Only add a newline if progress was actually rendered.
		progressRendered.Store(false) // Mark the newline as handled to avoid a duplicate newline on done.
	}
}

// resumeProgress resumes progress rendering.
func resumeProgress() {
	progressRendered.Store(false)
	progressPaused.Store(false)
}

// Helpers.
func (pc *ProxyChecker) incrementProgress() {
	atomic.AddInt32(&pc.progress, 1)
	Progress.Add(1)
}

func (pc *ProxyChecker) incrementAvailable() {
	atomic.AddInt32(&pc.available, 1)
	Available.Add(1)
}

func (pc *ProxyChecker) resetPhaseCounters(count int) {
	// Cancellation is scoped per-phase via installPhaseCancel, no reset needed here.
	pc.proxyCount = count
	atomic.StoreInt32(&pc.progress, 0)
	atomic.StoreInt32(&pc.available, 0)
	Progress.Store(0)
	Available.Store(0)
	ProxyCount.Store(uint32(count))

	// Reset downstream pipeline counters as well so a fresh run doesn't
	// inherit totals from a previous one (affects both the web admin UI
	// and the three-line CLI progress renderer).
	MediaDone.Store(0)
	FilterPassed.Store(0)
	SpeedDone.Store(0)
	SpeedOk.Store(0)
}

// checkSubscriptionSuccessRate checks subscription success rates and logs warnings.
func (pc *ProxyChecker) checkSubscriptionSuccessRate(allProxies []map[string]any) {
	// Count total and successful nodes for each subscription.
	subStats := make(map[string]struct {
		total   int
		success int
	})

	// Count subscription sources for all nodes.
	for _, proxy := range allProxies {
		if subUrl, ok := proxy["sub_url"].(string); ok {
			stats := subStats[subUrl]
			stats.total++
			subStats[subUrl] = stats
		}
	}

	// Count subscription sources for successful nodes.
	for _, result := range pc.results {
		if result.Proxy != nil {
			if subUrl, ok := result.Proxy["sub_url"].(string); ok {
				stats := subStats[subUrl]
				stats.success++
				subStats[subUrl] = stats
			}
			delete(result.Proxy, "sub_url")
			// Preserve node tags in 127.0.0.1:8199/sub/all.yaml.
			if subTag, ok := result.Proxy["sub_tag"].(string); ok {
				if subTag == "" {
					delete(result.Proxy, "sub_tag")
				}
			}
		}
	}

	// Check success rates and log warnings.
	for subUrl, stats := range subStats {
		if stats.total > 0 {
			successRate := float32(stats.success) / float32(stats.total)

			// Warn when the success rate is below the configured threshold.
			if successRate < config.GlobalConfig.SuccessRate {
				slog.Warn(fmt.Sprintf("Subscription success rate is too low: %s", subUrl),
					"total-nodes", stats.total,
					"successful-nodes", stats.success,
					"success-rate", fmt.Sprintf("%.2f%%", successRate*100))
			} else {
				slog.Debug(fmt.Sprintf("Subscription node stats: %s", subUrl),
					"total-nodes", stats.total,
					"successful-nodes", stats.success,
					"success-rate", fmt.Sprintf("%.2f%%", successRate*100))
			}
		}
	}
}

// statsConn wraps net.Conn to count bytes read and apply rate limiting
type statsConn struct {
	net.Conn
	bytesRead *uint64
	bucket    *ratelimit.Bucket
}

func (c *statsConn) Read(b []byte) (n int, err error) {
	// Global speed limit.
	if c.bucket != nil {
		c.bucket.Wait(int64(len(b)))
	}

	n, err = c.Conn.Read(b)
	atomic.AddUint64(c.bytesRead, uint64(n))

	return n, err
}

// CreateClient creates and returns an http.Client with a Close function
type ProxyClient struct {
	*http.Client
	proxy     constant.Proxy
	BytesRead *uint64
}

func CreateClient(mapping map[string]any) *ProxyClient {
	proxy, err := adapter.ParseProxy(mapping)
	if err != nil {
		slog.Debug("Failed to create mihomo client", "proxy", mapping["name"], "err", err)
		return nil
	}

	var bytesRead uint64
	baseTransport := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			if err := waitForProbeSlot(ctx); err != nil {
				return nil, err
			}

			host, port, err := net.SplitHostPort(addr)
			if err != nil {
				return nil, err
			}
			var u16Port uint16
			if port, err := strconv.ParseUint(port, 10, 16); err == nil {
				u16Port = uint16(port)
			}
			conn, err := proxy.DialContext(ctx, &constant.Metadata{
				Host:    host,
				DstPort: u16Port,
			})
			if err != nil {
				return nil, err
			}
			return &statsConn{
				Conn:      conn,
				bytesRead: &bytesRead,
				bucket:    Bucket,
			}, nil
		},
		DisableKeepAlives: true,
	}

	return &ProxyClient{
		Client: &http.Client{
			Timeout:   time.Duration(config.GlobalConfig.Timeout) * time.Millisecond,
			Transport: baseTransport,
		},
		proxy:     proxy,
		BytesRead: &bytesRead,
	}
}

// Close closes the proxy client and cleans up resources
// Manually close resources to guard against leaks in lower-level libraries.
func (pc *ProxyClient) Close() {
	if pc.Client != nil {
		pc.Client.CloseIdleConnections()
	}

	// Even if not closed here, lower-level GC eventually closes it.
	// Closing promptly helps memory recovery. Some transports can block on
	// Close, so give up after the timeout and leave cleanup to GC.
	if pc.proxy != nil {
		proxy := pc.proxy
		done := make(chan struct{})
		go func() {
			proxy.Close()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			slog.Debug(fmt.Sprintf("Timed out closing proxy connection; leaving cleanup to GC: %v", proxy))
		}
	}
	pc.Client = nil

	if pc.BytesRead != nil {
		TotalBytes.Add(atomic.LoadUint64(pc.BytesRead))
	}
}

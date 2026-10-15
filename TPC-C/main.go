// tpccReplay — replays the DePart TPC-C T1-T8 trace (tpcc_trace.csv, written
// by `TPCC_Main oasis ...`) against OasisDB's batchManager, open loop at a
// fixed offered rate, and reports throughput and latency in the same terms as
// the DePart TPC-C runs.
//
// It talks to the batchManager directly (the same AddKeys RPC the resolver
// uses after it has resolved a query to keys), so OasisDB executes exactly
// the record-level key set Q that DePart and the index baseline execute.
// Batching, fake requests, and table -> executor placement are OasisDB's own.
//
// Build (inside the ObliSQL repo):   cd cmd/tpccReplay && go build .
// Run:
//   ./tpccReplay -bh 127.0.0.1 -bp 9500 -trace tpcc_trace.csv -rate 50 -t 60 -warm 10 -out oasis_r50
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	loadBalancer "github.com/project/ObliSql/api/loadbalancer"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type op struct {
	stage int
	write bool
	key   string
	value string
}

type txn struct {
	id     int
	typ    string
	stages [][]op // ordered by stage; empty stage list => no ORAM ops (T1/T2)
	nOps   int
}

type result struct {
	typ      string
	arrive   time.Duration // since start
	latency  time.Duration
	ok       bool
	nOps     int
}

func loadTrace(path string) ([]txn, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	byID := map[int]*txn{}
	stageOps := map[int]map[int][]op{}
	order := []int{}
	first := true
	for sc.Scan() {
		line := sc.Text()
		if first { // header
			first = false
			continue
		}
		// txn_id,type,stage,op,key,value   (value may contain commas)
		parts := strings.SplitN(line, ",", 6)
		if len(parts) < 5 {
			continue
		}
		id, _ := strconv.Atoi(parts[0])
		st, _ := strconv.Atoi(parts[2])
		if _, ok := byID[id]; !ok {
			byID[id] = &txn{id: id, typ: parts[1]}
			stageOps[id] = map[int][]op{}
			order = append(order, id)
		}
		if parts[3] == "N" {
			continue
		}
		val := ""
		if len(parts) == 6 {
			val = parts[5]
		}
		stageOps[id][st] = append(stageOps[id][st], op{stage: st, write: parts[3] == "W", key: parts[4], value: val})
		byID[id].nOps++
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	sort.Ints(order)
	out := make([]txn, 0, len(order))
	for _, id := range order {
		t := byID[id]
		sts := []int{}
		for s := range stageOps[id] {
			sts = append(sts, s)
		}
		sort.Ints(sts)
		for _, s := range sts {
			t.stages = append(t.stages, stageOps[id][s])
		}
		out = append(out, *t)
	}
	return out, nil
}

func quantile(v []float64, q float64) float64 {
	if len(v) == 0 {
		return 0
	}
	s := append([]float64(nil), v...)
	sort.Float64s(s)
	i := int(q * float64(len(s)-1))
	return s[i]
}

func main() {
	bh := flag.String("bh", "127.0.0.1", "batchManager host(s), comma-separated")
	bp := flag.String("bp", "9500", "batchManager port(s), comma-separated")
	tracePath := flag.String("trace", "tpcc_trace.csv", "trace from `TPCC_Main oasis`")
	rate := flag.Float64("rate", 50, "offered load (txn/s), open loop")
	dur := flag.Int("t", 60, "measured duration (s)")
	warm := flag.Int("warm", 10, "warm-up (s), excluded from results")
	maxIn := flag.Int("maxinflight", 20000, "txns in flight before new arrivals are dropped")
	timeout := flag.Int("timeout", 120, "per-stage RPC timeout (s)")
	out := flag.String("out", "oasis", "output prefix")
	system := flag.String("system", "OasisDB", "system label in the CSV")
	flag.Parse()

	txns, err := loadTrace(*tracePath)
	if err != nil || len(txns) == 0 {
		fmt.Fprintf(os.Stderr, "cannot load trace %s: %v\n", *tracePath, err)
		os.Exit(1)
	}
	fmt.Printf("loaded %d txns from %s\n", len(txns), *tracePath)

	hosts, ports := strings.Split(*bh, ","), strings.Split(*bp, ",")
	if len(hosts) != len(ports) {
		fmt.Fprintln(os.Stderr, "-bh and -bp must have the same length")
		os.Exit(1)
	}
	clients := []loadBalancer.LoadBalancerClient{}
	for i := range hosts {
		addr := hosts[i] + ":" + ports[i]
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(600*1024*1024), grpc.MaxCallSendMsgSize(600*1024*1024)))
		if err != nil {
			fmt.Fprintf(os.Stderr, "connect %s: %v\n", addr, err)
			os.Exit(1)
		}
		c := loadBalancer.NewLoadBalancerClient(conn)
		if r, err := c.ConnectPing(context.Background(), &loadBalancer.ClientConnect{Id: "1"}); err != nil || r.Id != "1" {
			fmt.Fprintf(os.Stderr, "ping to batchManager %s failed: %v\n", addr, err)
			os.Exit(1)
		}
		clients = append(clients, c)
	}
	fmt.Printf("connected to %d batchManager(s); offered=%.1f txn/s  warm=%ds  measured=%ds\n", len(clients), *rate, *warm, *dur)

	var reqID atomic.Int64
	var inflight atomic.Int64
	var dropped atomic.Int64
	results := make(chan result, 1<<16)
	var wg sync.WaitGroup

	runTxn := func(t txn, arrive time.Duration, start time.Time) {
		defer wg.Done()
		defer inflight.Add(-1)
		ok := true
		for _, st := range t.stages {
			req := &loadBalancer.LoadBalanceRequest{
				RequestId: reqID.Add(1), ObjectNum: 1, TotalObjects: 1,
				Keys: make([]string, 0, len(st)), Values: make([]string, 0, len(st)),
			}
			for _, o := range st {
				req.Keys = append(req.Keys, o.key)
				if o.write {
					req.Values = append(req.Values, o.value)
				} else {
					req.Values = append(req.Values, "")
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*timeout)*time.Second)
			_, err := clients[rand.Intn(len(clients))].AddKeys(ctx, req)
			cancel()
			if err != nil {
				ok = false
				break
			}
		}
		// latency from the SCHEDULED arrival (open loop: includes queueing)
		results <- result{typ: t.typ, arrive: arrive, latency: time.Since(start) - arrive, ok: ok, nOps: t.nOps}
	}

	// collector
	var all []result
	collDone := make(chan struct{})
	go func() {
		for r := range results {
			all = append(all, r)
		}
		close(collDone)
	}()

	start := time.Now()
	total := time.Duration(*warm+*dur) * time.Second
	interval := time.Duration(float64(time.Second) / *rate)
	next := 0
	for i := 0; ; i++ {
		arrive := time.Duration(i) * interval
		if arrive >= total {
			break
		}
		if d := time.Until(start.Add(arrive)); d > 0 {
			time.Sleep(d)
		}
		t := txns[next%len(txns)]
		next++
		if inflight.Load() >= int64(*maxIn) {
			if arrive >= time.Duration(*warm)*time.Second {
				dropped.Add(1)
			}
			continue
		}
		inflight.Add(1)
		wg.Add(1)
		go runTxn(t, arrive, start)
	}
	fmt.Printf("arrivals done; waiting for %d in-flight txns\n", inflight.Load())
	wg.Wait()
	close(results)
	<-collDone

	// ── summarise the measured window ─────────────────────────────────────
	lo := time.Duration(*warm) * time.Second
	hi := lo + time.Duration(*dur)*time.Second
	// throughput counts txns that COMPLETED inside the measured window, so an
	// overloaded run cannot report "throughput = offered" just because the
	// backlog drained after the window closed.
	type agg struct {
		n, ok, fail, ops, win int
		lat              []float64
	}
	byType := map[string]*agg{}
	tot := &agg{}
	for _, r := range all {
		if r.arrive < lo {
			continue
		}
		for _, a := range []*agg{tot, func() *agg {
			if byType[r.typ] == nil {
				byType[r.typ] = &agg{}
			}
			return byType[r.typ]
		}()} {
			a.n++
			if r.ok {
				a.ok++
				if r.arrive+r.latency <= hi {
					a.win++
				}
				a.ops += r.nOps
				a.lat = append(a.lat, r.latency.Seconds())
			} else {
				a.fail++
			}
		}
	}
	d := float64(*dur)
	f, _ := os.Create(*out + "_summary.csv")
	fmt.Fprintln(f, "system,offered_txn_s,duration_s,arrived,completed,failed,dropped,throughput_txn_s,lat_p50_s,lat_p95_s,lat_p99_s,record_ops_per_txn")
	opsPer := 0.0
	if tot.ok > 0 {
		opsPer = float64(tot.ops) / float64(tot.ok)
	}
	fmt.Fprintf(f, "%s,%.1f,%d,%d,%d,%d,%d,%.2f,%.4f,%.4f,%.4f,%.2f\n", *system, *rate, *dur, tot.n, tot.ok, tot.fail,
		dropped.Load(), float64(tot.win)/d, quantile(tot.lat, .5), quantile(tot.lat, .95), quantile(tot.lat, .99), opsPer)
	f.Close()

	g, _ := os.Create(*out + "_types.csv")
	fmt.Fprintln(g, "system,offered_txn_s,txn_type,arrived,completed,failed,throughput_txn_s,lat_p50_s,lat_p95_s")
	names := []string{}
	for k := range byType {
		names = append(names, k)
	}
	sort.Strings(names)
	for _, k := range names {
		a := byType[k]
		fmt.Fprintf(g, "%s,%.1f,%s,%d,%d,%d,%.2f,%.4f,%.4f\n", *system, *rate, k, a.n, a.ok, a.fail,
			float64(a.win)/d, quantile(a.lat, .5), quantile(a.lat, .95))
	}
	g.Close()

	fmt.Printf("\n%s @ %.0f txn/s: completed %d/%d (failed %d, dropped %d)  throughput %.1f txn/s  p50 %.3fs  p95 %.3fs\n",
		*system, *rate, tot.ok, tot.n, tot.fail, dropped.Load(), float64(tot.win)/d, quantile(tot.lat, .5), quantile(tot.lat, .95))
	fmt.Printf("wrote %s_summary.csv, %s_types.csv\n", *out, *out)
}

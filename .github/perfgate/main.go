// Command perfgate is the CI performance gate. It reads `go test -bench
// -benchmem` output for a head revision (and optionally its base revision)
// and fails when
//
//   - a benchmark's median allocs/op exceeds its budget (budget.json), or
//     grows more than -max-alloc-growth over the base, or
//   - a benchmark's median jobs/s drops more than -max-slowdown below the base.
//
// Allocation counts are deterministic, so their limits are tight. Throughput
// on shared CI runners is noisy, so it is compared only against the base
// measured on the same runner, as medians of interleaved runs, with a wide
// tolerance.
//
//	go run ./.github/perfgate -head head.txt -base base.txt -budget .github/perfgate/budget.json
//
// The report is Markdown, ready for $GITHUB_STEP_SUMMARY. It lives under
// .github so `./...` (tests, lint) never picks it up.
package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
)

// result holds every sample of one benchmark, per unit ("allocs/op", ...).
type result map[string][]float64

// benchLine matches a result line: name-procs, iterations, then value/unit
// pairs. Lines broken by interleaved log output simply do not match.
var benchLine = regexp.MustCompile(`^(Benchmark\S+?)(?:-\d+)?\s+\d+\s+(.+)$`)

func parse(r io.Reader) (map[string]result, error) {
	out := map[string]result{}
	sc := bufio.NewScanner(r)

	for sc.Scan() {
		m := benchLine.FindStringSubmatch(strings.TrimSpace(sc.Text()))
		if m == nil {
			continue
		}

		fields := strings.Fields(m[2])
		if len(fields)%2 != 0 {
			continue
		}

		res := out[m[1]]
		if res == nil {
			res = result{}
			out[m[1]] = res
		}

		for i := 0; i < len(fields); i += 2 {
			v, err := strconv.ParseFloat(fields[i], 64)
			if err != nil {
				continue
			}

			res[fields[i+1]] = append(res[fields[i+1]], v)
		}
	}

	return out, sc.Err()
}

func parseFile(path string) (map[string]result, error) {
	f, err := os.Open(path) //nolint:gosec // G304: path comes from the CI command line
	if err != nil {
		return nil, err
	}
	defer f.Close()

	return parse(f)
}

func median(xs []float64) (float64, bool) {
	if len(xs) == 0 {
		return 0, false
	}

	s := slices.Clone(xs)
	slices.Sort(s)

	if n := len(s); n%2 == 1 {
		return s[n/2], true
	}

	return (s[len(s)/2-1] + s[len(s)/2]) / 2, true
}

type gate struct {
	budget         map[string]float64
	maxSlowdown    float64
	maxAllocGrowth float64
}

type row struct {
	name                   string
	headJobs, baseJobs     float64
	headAllocs, baseAllocs float64
	budget                 float64
	hasBase, hasJobs       bool
	failures               []string
}

func (g gate) check(head, base map[string]result) ([]row, error) {
	if len(head) == 0 {
		return nil, errors.New("no benchmark results in head output")
	}

	names := make([]string, 0, len(head))
	for n := range head {
		names = append(names, n)
	}

	sort.Strings(names)

	rows := make([]row, 0, len(names))

	for _, n := range names {
		r := row{name: n, budget: g.budget[n]}
		r.headAllocs, _ = median(head[n]["allocs/op"])
		r.headJobs, r.hasJobs = median(head[n]["jobs/s"])

		if b, ok := base[n]; ok {
			r.baseAllocs, _ = median(b["allocs/op"])
			r.baseJobs, _ = median(b["jobs/s"])
			r.hasBase = true
		}

		if r.budget > 0 && r.headAllocs > r.budget {
			r.failures = append(r.failures, fmt.Sprintf("allocs/op %.0f > budget %.0f", r.headAllocs, r.budget))
		}

		if r.hasBase && r.baseAllocs > 0 && r.headAllocs > r.baseAllocs*(1+g.maxAllocGrowth) {
			r.failures = append(r.failures, fmt.Sprintf("allocs/op %.0f > base %.0f +%.0f%%", r.headAllocs, r.baseAllocs, g.maxAllocGrowth*100))
		}

		if r.hasBase && r.hasJobs && r.baseJobs > 0 && r.headJobs < r.baseJobs*(1-g.maxSlowdown) {
			r.failures = append(r.failures, fmt.Sprintf("jobs/s %.0f < base %.0f -%.0f%%", r.headJobs, r.baseJobs, g.maxSlowdown*100))
		}

		rows = append(rows, r)
	}

	for n := range g.budget {
		if _, ok := head[n]; !ok {
			return rows, fmt.Errorf("budgeted benchmark %s did not run", n)
		}
	}

	return rows, nil
}

func report(w io.Writer, rows []row) int {
	failed := 0

	fmt.Fprintln(w, "### Performance gate")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "| Benchmark | jobs/s base | jobs/s head | Δ | allocs/op base | allocs/op head | budget | result |")
	fmt.Fprintln(w, "| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |")

	for _, r := range rows {
		baseJobs, delta, baseAllocs, budget := "–", "–", "–", "–"

		if r.hasBase {
			baseJobs = fmt.Sprintf("%.0f", r.baseJobs)
			baseAllocs = fmt.Sprintf("%.0f", r.baseAllocs)

			if r.baseJobs > 0 {
				delta = fmt.Sprintf("%+.1f%%", (r.headJobs/r.baseJobs-1)*100)
			}
		}

		if r.budget > 0 {
			budget = fmt.Sprintf("%.0f", r.budget)
		}

		status := "✅"
		if len(r.failures) > 0 {
			status = "❌ " + strings.Join(r.failures, "; ")
			failed++
		}

		fmt.Fprintf(w, "| `%s` | %s | %.0f | %s | %s | %.0f | %s | %s |\n",
			strings.TrimPrefix(r.name, "Benchmark"), baseJobs, r.headJobs, delta, baseAllocs, r.headAllocs, budget, status)
	}

	fmt.Fprintln(w)

	return failed
}

func run() error {
	var (
		headPath   = flag.String("head", "", "benchmark output of the revision under test (required)")
		basePath   = flag.String("base", "", "benchmark output of the base revision (optional)")
		budgetPath = flag.String("budget", "", "JSON object of benchmark name -> max allocs/op")
		g          gate
	)

	flag.Float64Var(&g.maxSlowdown, "max-slowdown", 0.25, "max allowed jobs/s drop vs base (0.25 = 25%)")
	flag.Float64Var(&g.maxAllocGrowth, "max-alloc-growth", 0.10, "max allowed allocs/op growth vs base")
	flag.Parse()

	if *headPath == "" {
		return errors.New("-head is required")
	}

	head, err := parseFile(*headPath)
	if err != nil {
		return fmt.Errorf("head: %w", err)
	}

	var base map[string]result

	if *basePath != "" {
		if base, err = parseFile(*basePath); err != nil {
			return fmt.Errorf("base: %w", err)
		}
	}

	if *budgetPath != "" {
		b, err := os.ReadFile(*budgetPath)
		if err != nil {
			return fmt.Errorf("budget: %w", err)
		}

		if err := json.Unmarshal(b, &g.budget); err != nil {
			return fmt.Errorf("budget: %w", err)
		}
	}

	rows, checkErr := g.check(head, base)
	if failed := report(os.Stdout, rows); failed > 0 {
		return fmt.Errorf("%d benchmark(s) failed the performance gate", failed)
	}

	return checkErr
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "perfgate:", err)
		os.Exit(1)
	}
}

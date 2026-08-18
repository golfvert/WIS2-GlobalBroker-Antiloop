// wis2nodes is a standalone, read-only diagnostic — connects to the
// same Redis Cluster antiloop itself uses and reports, per centre_id,
// which host currently holds primary/secondary and how many election
// participants look alive right now. Run from an operator's own
// machine against the remote fleet's Redis; not deployed to waloop
// hosts, not part of the fleet itself — same category as cmd/redislat
// and cmd/setnxtime.
//
// REDIS_URL/REDIS_CLUSTER/ELECTION_FRESH_WINDOW come from the real
// deployed common.env (see defaultEnvPath below), loaded via
// internal/envfile the same way cmd/antiloop loads it — not from shell
// env vars or CLI flags, so there's nothing to export before running
// this. -env overrides which file to read, if ever needed.
//
// Data sources, both written by cmd/antiloop (see main.go's "Election
// (embedded...)" block and internal/election/client.go):
//
//   - "wis2gb:instances" (HASH) — THE source for PRIMARY/SECONDARY and
//     their SINCE times. Fields "<centre_id>_primary"/"_secondary"
//     hold the current holder's host, "..._time" siblings the unix-ms
//     of the last write.
//
//     Whichever host currently holds a role re-HSETs it unconditionally
//     every ELECTION_INTERVAL, not just on a role transition — matching
//     the original Node-RED flow's own periodic inject, which re-runs
//     Elect and re-writes every cycle regardless of whether the role
//     actually changed. That's what makes these fields self-healing:
//     the current holder is always the most recent writer, so any
//     stale/wrong value (e.g. from the brief startup race where an
//     instance can transiently pass through the wrong role before
//     settling) only survives one cycle before being overwritten again
//     — these fields can be trusted directly, with no need to
//     cross-reference them against wis2gb:election below. Staleness (a
//     field whose "_time" hasn't been refreshed in a while — the
//     role-holder stopped) is shown via the SINCE column's own age,
//     dimmed/reddened past ELECTION_FRESH_WINDOW, rather than by hiding
//     the host.
//
//   - "wis2gb:election:<centre_id>" (HASH) — uuid -> "<ms>|<host>",
//     refreshed every ELECTION_INTERVAL by every live instance,
//     opportunistically pruned of stale entries. Used only for the
//     LIVE column here (how many participants are currently
//     announcing) — a count, so it doesn't need the embedded host at
//     all, just how many entries are fresher than
//     ELECTION_FRESH_WINDOW.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"antiloop/internal/envfile"
	"antiloop/internal/redisconn"

	"github.com/redis/go-redis/v9"
)

// defaultEnvPath mirrors build.sh's own "$dir/../deploy" convention:
// this tool is local/operator-run only (never deployed to the fleet
// itself), so it assumes it's being run from source/ in this repo,
// right next to a sibling deploy/. ANTILOOP_DEPLOY_DIR overrides that
// assumption once (e.g. set in your shell profile if you run this
// from elsewhere); -env overrides the whole path directly for a
// one-off. Change the env var, not this code, to repoint it.
func defaultEnvPath() string {
	deployDir := os.Getenv("ANTILOOP_DEPLOY_DIR")
	if deployDir == "" {
		deployDir = filepath.Join("..", "deploy")
	}
	return filepath.Join(deployDir, "shared", "common.env")
}

type nodeStatus struct {
	centreID string

	primaryHost   string
	primaryTime   time.Time
	secondaryHost string
	secondaryTime time.Time

	// live/liveKnown come from wis2gb:election:<centre_id> — see
	// fetchLiveState. liveKnown is false only if that centre's HGETALL
	// itself failed (a real Redis error, not merely an empty/absent
	// key).
	live      int
	liveKnown bool
}

func main() {
	envPath := flag.String("env", defaultEnvPath(), "common.env-format file to read REDIS_URL/REDIS_CLUSTER/ELECTION_FRESH_WINDOW from")
	timeout := flag.Duration("timeout", 15*time.Second, "overall Redis operation timeout")
	noColor := flag.Bool("no-color", os.Getenv("NO_COLOR") != "", "disable ANSI colors")
	dumpID := flag.String("dump", "", "diagnostic: print the raw wis2gb:election:<centre_id> hash (per-uuid age, TTL) plus every wis2gb:instances field for one centre_id, and exit — redis-cli isn't installed on the waloop hosts, this is the substitute")
	delID := flag.String("del", "", "remove wis2gb:instances' 4 fields, and wis2gb:election:<centre_id> if present, for one centre_id (e.g. a decommissioned test node stuck showing 0 live) — prints what it would delete and exits WITHOUT deleting unless -yes is also passed")
	yes := flag.Bool("yes", false, "actually perform the -del deletion — without this, -del only shows what it would delete (dry run)")
	flag.Parse()

	// envfile.Load sets REDIS_URL, REDIS_CLUSTER, ELECTION_FRESH_WINDOW
	// (and everything else in the file) into this process's
	// environment, same as cmd/antiloop does with the real deployed
	// common.env, so nothing below has to be separately typed as a
	// shell env var or CLI flag before running this. -env only exists
	// to point at a different file — it does not take the values
	// themselves.
	if err := envfile.Load(*envPath); err != nil {
		log.Fatalf("loading %s: %v (pass -env to point at a different common.env)", *envPath, err)
	}

	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		log.Fatalf("REDIS_URL not set after loading %s", *envPath)
	}
	// REDIS_CLUSTER, default true — matches config.Config's default.
	cluster := os.Getenv("REDIS_CLUSTER") != "false"

	freshWindow := 8 * time.Second
	if v := os.Getenv("ELECTION_FRESH_WINDOW"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			freshWindow = d
		}
	}

	rdb, err := redisconn.New(redisURL, 1, cluster)
	if err != nil {
		log.Fatalf("redisconn.New: %v", err)
	}
	defer rdb.Close()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("redis ping failed: %v", err)
	}

	if *dumpID != "" {
		runDump(ctx, rdb, *dumpID)
		return
	}

	if *delID != "" {
		runDel(ctx, rdb, *delID, *yes)
		return
	}

	instances, err := rdb.HGetAll(ctx, "wis2gb:instances").Result()
	if err != nil {
		log.Fatalf("HGETALL wis2gb:instances: %v", err)
	}
	if len(instances) == 0 {
		fmt.Println("wis2gb:instances is empty — no centre has completed a role election yet (or this is the wrong Redis).")
		return
	}

	nodes := parseInstances(instances)

	order := make([]string, 0, len(nodes))
	for id := range nodes {
		order = append(order, id)
	}
	sort.Strings(order)

	fetchLiveState(ctx, rdb, nodes, order, freshWindow)

	printTable(nodes, order, freshWindow, !*noColor)
}

// runDump prints exactly what's in Redis right now for one centre:
// every wis2gb:election:<centre_id> field (uuid, raw value, age, TTL
// of the whole key) with NO freshness filtering, plus every
// wis2gb:instances field for that centre_id (host/time, with age).
func runDump(ctx context.Context, rdb redis.UniversalClient, centreID string) {
	key := "wis2gb:election:" + centreID
	all, err := rdb.HGetAll(ctx, key).Result()
	if err != nil {
		log.Fatalf("HGETALL %s: %v", key, err)
	}
	ttl, err := rdb.TTL(ctx, key).Result()
	if err != nil {
		log.Fatalf("TTL %s: %v", key, err)
	}

	now := time.Now()

	fmt.Printf("%s\n", key)
	switch {
	case ttl < 0 && len(all) == 0:
		fmt.Println("  key does not exist")
	case ttl < 0:
		fmt.Println("  TTL: none set (unexpected — every write to this key should refresh its EXPIRE, see internal/election)")
	default:
		fmt.Printf("  TTL: %s remaining\n", ttl.Round(time.Second))
	}
	if len(all) == 0 {
		fmt.Println("  no fields")
	} else {
		fmt.Printf("  %d field(s):\n", len(all))
		for uuid, raw := range all {
			tsStr, host, found := strings.Cut(raw, "|")
			ts, err := strconv.ParseInt(tsStr, 10, 64)
			if err != nil {
				fmt.Printf("    %s = %q (unparseable)\n", uuid, raw)
				continue
			}
			age := now.Sub(time.UnixMilli(ts)).Round(time.Second)
			hostDesc := host
			if !found {
				hostDesc = "(legacy — no host embedded)"
			} else if host == "" {
				hostDesc = "(empty host)"
			}
			fmt.Printf("    %s = %q -> age=%s host=%s\n", uuid, raw, age, hostDesc)
		}
	}

	instances, err := rdb.HGetAll(ctx, "wis2gb:instances").Result()
	if err != nil {
		log.Fatalf("HGETALL wis2gb:instances: %v", err)
	}
	fmt.Println("wis2gb:instances (this centre_id only):")
	prefix := centreID + "_"
	found := false
	for field, val := range instances {
		if !strings.HasPrefix(field, prefix) {
			continue
		}
		found = true
		if strings.HasSuffix(field, "_time") {
			if ms, err := strconv.ParseInt(val, 10, 64); err == nil {
				fmt.Printf("  %s = %s (age=%s)\n", field, val, now.Sub(time.UnixMilli(ms)).Round(time.Second))
				continue
			}
		}
		fmt.Printf("  %s = %s\n", field, val)
	}
	if !found {
		fmt.Println("  (no fields for this centre_id)")
	}
}

// runDel removes one centre_id's leftover Redis state — the 4
// wis2gb:instances fields, and wis2gb:election:<centre_id> if it still
// exists (it also carries its own 1h EXPIRE — see internal/election —
// so this just saves waiting). Dry-run unless confirmed: prints
// exactly what it would touch either way, only actually calls
// HDEL/DEL when confirmed is true. There's no per-centre HDEL for the
// housekeeping cmd/antiloop itself now does (primary auto-removes a
// stale secondary — see cmd/antiloop/main.go) because that only
// clears the SECONDARY slot, and only while some primary is still
// running to notice; a fully-stopped centre (0 live, like
// io-wis2dev-10-test-node) has nothing left running to do that
// cleanup, hence this manual escape hatch.
func runDel(ctx context.Context, rdb redis.UniversalClient, centreID string, confirmed bool) {
	fields := []string{
		centreID + "_primary", centreID + "_primary_time",
		centreID + "_secondary", centreID + "_secondary_time",
	}
	instances, err := rdb.HGetAll(ctx, "wis2gb:instances").Result()
	if err != nil {
		log.Fatalf("HGETALL wis2gb:instances: %v", err)
	}

	present := make([]string, 0, len(fields))
	fmt.Println("wis2gb:instances fields:")
	for _, f := range fields {
		if v, ok := instances[f]; ok {
			fmt.Printf("  %s = %s\n", f, v)
			present = append(present, f)
		}
	}
	if len(present) == 0 {
		fmt.Println("  (none set)")
	}

	electionKey := "wis2gb:election:" + centreID
	electionExists, err := rdb.Exists(ctx, electionKey).Result()
	if err != nil {
		log.Fatalf("EXISTS %s: %v", electionKey, err)
	}
	if electionExists > 0 {
		fmt.Printf("%s: exists, would DEL (whole key)\n", electionKey)
	} else {
		fmt.Printf("%s: does not exist\n", electionKey)
	}

	if len(present) == 0 && electionExists == 0 {
		fmt.Println("\nnothing to delete.")
		return
	}

	if !confirmed {
		fmt.Println("\ndry run — nothing deleted. Rerun with -yes to actually delete.")
		return
	}

	if len(present) > 0 {
		if err := rdb.HDel(ctx, "wis2gb:instances", present...).Err(); err != nil {
			log.Fatalf("HDEL wis2gb:instances: %v", err)
		}
	}
	if electionExists > 0 {
		if err := rdb.Del(ctx, electionKey).Err(); err != nil {
			log.Fatalf("DEL %s: %v", electionKey, err)
		}
	}
	fmt.Println("\ndeleted.")
}

// parseInstances turns the flat wis2gb:instances hash into one
// nodeStatus per centre_id — see package doc comment for why these
// fields (self-healing, rewritten every tick by whoever currently
// holds each role) are trusted directly now.
func parseInstances(instances map[string]string) map[string]*nodeStatus {
	nodes := make(map[string]*nodeStatus)
	get := func(id string) *nodeStatus {
		n, ok := nodes[id]
		if !ok {
			n = &nodeStatus{centreID: id}
			nodes[id] = n
		}
		return n
	}

	for key, val := range instances {
		switch {
		case strings.HasSuffix(key, "_primary_time"):
			id := strings.TrimSuffix(key, "_primary_time")
			if ms, err := strconv.ParseInt(val, 10, 64); err == nil {
				get(id).primaryTime = time.UnixMilli(ms)
			}
		case strings.HasSuffix(key, "_secondary_time"):
			id := strings.TrimSuffix(key, "_secondary_time")
			if ms, err := strconv.ParseInt(val, 10, 64); err == nil {
				get(id).secondaryTime = time.UnixMilli(ms)
			}
		case strings.HasSuffix(key, "_primary"):
			id := strings.TrimSuffix(key, "_primary")
			get(id).primaryHost = val
		case strings.HasSuffix(key, "_secondary"):
			id := strings.TrimSuffix(key, "_secondary")
			get(id).secondaryHost = val
		}
	}
	return nodes
}

// fetchLiveState pipelines one HGETALL per centre_id against
// wis2gb:election:<centre_id> — batched into a single round trip
// rather than one call per centre, since a full fleet can be a
// hundred-plus centres. Populates each nodeStatus's live (count of
// entries fresher than freshWindow) and liveKnown.
func fetchLiveState(ctx context.Context, rdb redis.UniversalClient, nodes map[string]*nodeStatus, order []string, freshWindow time.Duration) {
	pipe := rdb.Pipeline()
	cmds := make(map[string]*redis.MapStringStringCmd, len(order))
	for _, id := range order {
		cmds[id] = pipe.HGetAll(ctx, "wis2gb:election:"+id)
	}
	// Exec's error return is ignored deliberately: a single bad/missing
	// key surfaces as an empty map on its own Cmd, not a pipeline-wide
	// failure worth aborting the whole report over. Partial results
	// (LIVE shown as "?" per node — see liveKnown) are still more
	// useful than no report at all.
	_, _ = pipe.Exec(ctx)

	cutoff := time.Now().Add(-freshWindow).UnixMilli()
	for id, cmd := range cmds {
		all, err := cmd.Result()
		if err != nil && err != redis.Nil {
			continue // leave liveKnown false for this one
		}
		n := nodes[id]
		n.liveKnown = true
		for _, raw := range all {
			// "<ms>|<host>" (or a bare "<ms>" from an older build that
			// predates the embedded host) — only the timestamp matters
			// here, host is unused for a plain count.
			tsStr, _, _ := strings.Cut(raw, "|")
			ts, err := strconv.ParseInt(tsStr, 10, 64)
			if err != nil || ts < cutoff {
				continue
			}
			n.live++
		}
	}
}

const (
	colReset  = "\x1b[0m"
	colGreen  = "\x1b[32m"
	colYellow = "\x1b[33m"
	colRed    = "\x1b[31m"
	colCyan   = "\x1b[36m"
	colDim    = "\x1b[2m"
	colBold   = "\x1b[1m"
)

func printTable(nodes map[string]*nodeStatus, order []string, freshWindow time.Duration, color bool) {
	headers := []string{"CENTRE ID", "PRIMARY", "PRIMARY (last updated)", "SECONDARY", "SECONDARY (last updated)", "LIVE"}
	rows := make([][]string, 0, len(order))
	rowColors := make([][]string, 0, len(order)) // parallel color codes, "" = no color

	for _, id := range order {
		n := nodes[id]

		primaryDisplay, primarySince, primaryColor := roleCell(n.primaryHost, n.primaryTime, freshWindow, colGreen)
		secondaryDisplay, secondarySince, secondaryColor := roleCell(n.secondaryHost, n.secondaryTime, freshWindow, colCyan)

		live := "?"
		liveColor := colDim
		if n.liveKnown {
			live = strconv.Itoa(n.live)
			switch {
			case n.live >= 2:
				liveColor = colGreen
			case n.live == 1:
				liveColor = colYellow
			default:
				liveColor = colRed
			}
		}

		rows = append(rows, []string{id, primaryDisplay, primarySince, secondaryDisplay, secondarySince, live})
		rowColors = append(rowColors, []string{"", primaryColor, colDim, secondaryColor, colDim, liveColor})
	}

	widths := make([]int, len(headers))
	for i, h := range headers {
		widths[i] = utf8.RuneCountInString(h)
	}
	for _, row := range rows {
		for i, cell := range row {
			if w := utf8.RuneCountInString(cell); w > widths[i] {
				widths[i] = w
			}
		}
	}

	border := func(left, mid, right string) string {
		var b strings.Builder
		b.WriteString(left)
		for i, w := range widths {
			b.WriteString(strings.Repeat("─", w+2))
			if i < len(widths)-1 {
				b.WriteString(mid)
			}
		}
		b.WriteString(right)
		return b.String()
	}

	printRow := func(cells []string, colors []string, bold bool) {
		var b strings.Builder
		b.WriteString("│")
		for i, cell := range cells {
			pad := widths[i] - utf8.RuneCountInString(cell)
			text := cell
			if bold && color {
				text = colBold + text + colReset
			} else if color && colors != nil && colors[i] != "" {
				text = colors[i] + text + colReset
			}
			b.WriteString(" ")
			b.WriteString(text)
			b.WriteString(strings.Repeat(" ", pad))
			b.WriteString(" │")
		}
		fmt.Println(b.String())
	}

	fmt.Println(border("┌", "┬", "┐"))
	printRow(headers, nil, true)
	fmt.Println(border("├", "┼", "┤"))
	for i, row := range rows {
		printRow(row, rowColors[i], false)
	}
	fmt.Println(border("└", "┴", "┘"))

	total := len(order)
	fullyRedundant, exposed, dead := 0, 0, 0
	for _, id := range order {
		n := nodes[id]
		switch {
		case n.liveKnown && n.live >= 2:
			fullyRedundant++
		case n.liveKnown && n.live == 1:
			exposed++
		case n.liveKnown && n.live == 0:
			dead++
		}
	}
	fmt.Printf("\n%d centres — %d fully redundant (2 live), %d exposed (1 live), %d dead (0 live)\n",
		total, fullyRedundant, exposed, dead)
}

// roleCell renders one PRIMARY or SECONDARY cell straight from
// wis2gb:instances: "-" if that field has never been written for this
// centre, otherwise the host it names — colored/dimmed by how stale
// its own "_time" is (older than freshWindow means whoever wrote it
// has stopped re-asserting, i.e. it's probably dead, even though
// nothing has overwritten the field itself yet). The host and its last
// known time are always shown, never blanked out just for being stale
// — staleness is communicated through color/age, not by hiding data,
// since the field is self-healing on its own now (see package doc
// comment) rather than something this tool needs to second-guess.
func roleCell(host string, t time.Time, freshWindow time.Duration, freshColor string) (display, since, color string) {
	if host == "" {
		return "-", "-", colRed
	}
	since = humanSince(t)
	if !t.IsZero() && time.Since(t) <= freshWindow {
		return host, since, freshColor
	}
	return host, since, colDim
}

func humanSince(t time.Time) string {
	if t.IsZero() {
		return "-"
	}
	d := time.Since(t).Round(time.Second)
	if d < 0 {
		d = 0
	}
	days := d / (24 * time.Hour)
	d -= days * 24 * time.Hour
	hours := d / time.Hour
	d -= hours * time.Hour
	mins := d / time.Minute
	d -= mins * time.Minute
	secs := d / time.Second

	switch {
	case days > 0:
		return fmt.Sprintf("%dd%dh ago", days, hours)
	case hours > 0:
		return fmt.Sprintf("%dh%dm ago", hours, mins)
	case mins > 0:
		return fmt.Sprintf("%dm%ds ago", mins, secs)
	default:
		return fmt.Sprintf("%ds ago", secs)
	}
}

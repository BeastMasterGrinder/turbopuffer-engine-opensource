package main

import (
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/charmbracelet/bubbles/progress"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"github.com/farjad/turbopuffer-clone/internal/cache"
)

// isTerminal reports whether f is an interactive terminal. The live Bubble Tea
// dashboard only runs on a real TTY; piped/redirected output (and `go test`)
// fall back to plain periodic stderr lines so nothing hangs or renders garbage.
func isTerminal(f *os.File) bool {
	fi, err := f.Stat()
	if err != nil {
		return false
	}
	return fi.Mode()&os.ModeCharDevice != 0
}

// loadPhase is one concurrent query phase the dashboard tracks live. done is the
// shared counter the workers increment; the dashboard reads it on each tick.
type loadPhase struct {
	name  string
	total int64
	done  *atomic.Int64
}

// displayLoad runs work() to completion while showing progress. On a TTY it
// renders the Bubble Tea dashboard; otherwise it prints periodic stderr lines.
// cancel is invoked if the user aborts the dashboard (ctrl+c / q), so in-flight
// queries can stop. It returns only once work() has fully finished.
func displayLoad(title string, phases []loadPhase, store *cache.Store, cacheBefore cache.CacheStats, start time.Time, cancel func(), work func()) {
	if isTerminal(os.Stderr) {
		runDashboard(title, phases, store, cacheBefore, start, cancel, work)
		return
	}
	runPlain(title, phases, store, cacheBefore, start, work)
}

func runDashboard(title string, phases []loadPhase, store *cache.Store, cacheBefore cache.CacheStats, start time.Time, cancel func(), work func()) {
	workDone := make(chan struct{})
	p := tea.NewProgram(newDashboard(title, phases, store, cacheBefore, start), tea.WithOutput(os.Stderr))
	go func() {
		work()
		close(workDone)
		p.Send(doneMsg{}) // a no-op if the user already quit the program
	}()
	final, err := p.Run()
	if err == nil {
		if d, ok := final.(dashboard); ok && d.aborted {
			cancel() // ask the in-flight queries to stop
		}
	}
	<-workDone // never read results until work() has fully returned
}

func runPlain(title string, phases []loadPhase, store *cache.Store, cacheBefore cache.CacheStats, start time.Time, work func()) {
	stop := make(chan struct{})
	go func() {
		t := time.NewTicker(2 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				var done, total int64
				for _, p := range phases {
					done += p.done.Load()
					total += p.total
				}
				el := time.Since(start).Seconds()
				hot := store.Stats().Sub(cacheBefore).HitRate() * 100
				fmt.Fprintf(os.Stderr, "  …%s %d/%d (%.0f qps, cache %.0f%% hot)\n", title, done, total, float64(done)/el, hot)
			}
		}
	}()
	work()
	close(stop)
}

// dashboard is the Bubble Tea model: a live panel of per-phase progress bars plus
// rolled-up throughput, cache hit rate, and ETA. It reads the shared atomic
// counters and cache stats on every tick rather than receiving per-query
// messages, which keeps the worker hot path free of any UI coupling.
type dashboard struct {
	title       string
	phases      []loadPhase
	store       *cache.Store
	cacheBefore cache.CacheStats
	start       time.Time
	bar         progress.Model
	aborted     bool
}

type tickMsg time.Time
type doneMsg struct{}

func newDashboard(title string, phases []loadPhase, store *cache.Store, cacheBefore cache.CacheStats, start time.Time) dashboard {
	return dashboard{
		title:       title,
		phases:      phases,
		store:       store,
		cacheBefore: cacheBefore,
		start:       start,
		bar:         progress.New(progress.WithDefaultGradient(), progress.WithWidth(26)),
	}
}

func tick() tea.Cmd {
	return tea.Tick(150*time.Millisecond, func(t time.Time) tea.Msg { return tickMsg(t) })
}

func (m dashboard) Init() tea.Cmd { return tick() }

func (m dashboard) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case doneMsg:
		return m, tea.Quit
	case tickMsg:
		return m, tick()
	case tea.KeyMsg:
		if msg.Type == tea.KeyCtrlC || msg.String() == "q" {
			m.aborted = true
			return m, tea.Quit
		}
	}
	return m, nil
}

func (m dashboard) View() string {
	var doneAll, totalAll int64
	rows := ""
	for _, p := range m.phases {
		d := p.done.Load()
		doneAll += d
		totalAll += p.total
		pct := 0.0
		if p.total > 0 {
			pct = float64(d) / float64(p.total)
		}
		rows += fmt.Sprintf("%-10s %s %3.0f%%  %s/%s\n",
			p.name, m.bar.ViewAs(pct), pct*100, compact(d), compact(p.total))
	}

	el := time.Since(m.start).Seconds()
	qps := 0.0
	if el > 0 {
		qps = float64(doneAll) / el
	}
	hot := m.store.Stats().Sub(m.cacheBefore).HitRate() * 100
	eta := "—"
	if qps > 0 && doneAll < totalAll {
		eta = (time.Duration(float64(totalAll-doneAll)/qps) * time.Second).Round(time.Second).String()
	}
	footer := fmt.Sprintf("%.0f qps · cache %.1f%% hot · elapsed %s · ETA %s",
		qps, hot, time.Since(m.start).Round(time.Second), eta)

	body := lipgloss.NewStyle().Bold(true).Render("tpuf-bench · "+m.title) + "\n" + rows + "\n" + footer
	return lipgloss.NewStyle().Border(lipgloss.RoundedBorder()).Padding(0, 1).Render(body) + "\n"
}

// compact renders a count like 14.4k for readability.
func compact(n int64) string {
	if n >= 1000 {
		return fmt.Sprintf("%.1fk", float64(n)/1000)
	}
	return fmt.Sprintf("%d", n)
}

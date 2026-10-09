// Command reload-server is a stdlib-only static file server with live reload.
package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const reloadPath = "/__reload"

// reloader is injected into every page, to reload on our events
var reloader = []byte(`<script>new EventSource("` + reloadPath + `").onmessage = () => location.reload();</script>`)

// verbose enables logging beyond errors and the startup line.
var verbose bool

// logv logs only when verbose is set.
func logv(format string, args ...any) {
	if verbose {
		log.Printf(format, args...)
	}
}

func main() {
	port := flag.Int("port", 3000, "port to listen on")
	dir := flag.String("dir", ".", "directory to serve")
	poll := flag.Duration("poll", 300*time.Millisecond, "how often to check for file changes")
	flag.BoolVar(&verbose, "v", false, "verbose logging")
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "usage: %s [flags]\n\nflags:\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()

	if flag.NArg() > 0 {
		flag.Usage()
		os.Exit(2)
	}
	if *port < 1 || *port > 65535 {
		log.Fatalf("invalid port %d", *port)
	}
	if *poll <= 0 {
		log.Fatalf("invalid poll interval %v", *poll)
	}

	if err := run(*port, *dir, *poll); err != nil {
		log.Fatal(err)
	}
}

func run(port int, dir string, poll time.Duration) error {
	root, err := os.OpenRoot(dir)
	if err != nil {
		return err
	}
	defer root.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	s := newServer(root.FS())
	go s.watch(ctx, poll)

	mux := http.NewServeMux()
	mux.HandleFunc("GET "+reloadPath, s.serveEvents)
	mux.HandleFunc("GET /", s.serveFile)

	srv := &http.Server{
		Addr:    net.JoinHostPort("", strconv.Itoa(port)),
		Handler: mux,
		// Cancel request contexts on shutdown so open event streams return.
		BaseContext: func(net.Listener) context.Context { return ctx },
	}

	errc := make(chan error, 1)
	go func() {
		log.Printf("serving %s at http://localhost:%d", dir, port)
		errc <- srv.ListenAndServe()
	}()

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}

	logv("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return err
	}
	if err := <-errc; !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

type server struct {
	fsys fs.FS

	mu      sync.Mutex
	clients map[chan struct{}]struct{}
}

func newServer(fsys fs.FS) *server {
	return &server{
		fsys:    fsys,
		clients: make(map[chan struct{}]struct{}),
	}
}

// serveFile serves a file from s.fsys, injecting the reload script into HTML.
func (s *server) serveFile(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")

	name := strings.TrimPrefix(path.Clean(r.URL.Path), "/")
	if name == "" || strings.HasSuffix(r.URL.Path, "/") {
		name = path.Join(name, "index.html")
	}

	if !strings.EqualFold(path.Ext(name), ".html") {
		http.ServeFileFS(w, r, s.fsys, name)
		return
	}

	data, err := fs.ReadFile(s.fsys, name)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write(injectReloader(data))
}

// injectReloader before the closing body tag, appending it if there isn't one
func injectReloader(page []byte) []byte {
	i := bytes.LastIndex(page, []byte("</body>"))
	if i < 0 {
		return append(page, reloader...)
	}
	out := make([]byte, 0, len(page)+len(reloader))
	out = append(out, page[:i]...)
	out = append(out, reloader...)
	return append(out, page[i:]...)
}

// serveEvents handles the SSE stream
func (s *server) serveEvents(w http.ResponseWriter, r *http.Request) {
	rc := http.NewResponseController(w)

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	if err := rc.Flush(); err != nil {
		return
	}

	ch := s.subscribe()
	defer s.unsubscribe(ch)

	for {
		select {
		case <-ch:
			fmt.Fprint(w, "data: reload\n\n")
			if err := rc.Flush(); err != nil {
				return
			}
		case <-r.Context().Done():
			return
		}
	}
}

// subscribe registers a new client and returns the channel it is notified on.
func (s *server) subscribe() chan struct{} {
	ch := make(chan struct{}, 1)
	s.mu.Lock()
	s.clients[ch] = struct{}{}
	s.mu.Unlock()
	return ch
}

// unsubscribe removes the client registered with ch.
func (s *server) unsubscribe(ch chan struct{}) {
	s.mu.Lock()
	delete(s.clients, ch)
	s.mu.Unlock()
}

// broadcast notifies every client without a pending notification
func (s *server) broadcast() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for ch := range s.clients {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// watch polls s.fsys every interval and broadcasts on any file change.
func (s *server) watch(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	last := snapshot(s.fsys)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		cur := snapshot(s.fsys)
		if name, ok := firstChange(last, cur); ok {
			logv("changed: %s", name)
			s.broadcast()
		}
		last = cur
	}
}

// snapshot maps each file to its mod time, skipping dotfiles and node_modules.
func snapshot(fsys fs.FS) map[string]time.Time {
	files := make(map[string]time.Time)
	fs.WalkDir(fsys, ".", func(name string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil // Skip unreadable entries; they may be mid-write.
		}
		base := d.Name()
		if d.IsDir() {
			if name != "." && (strings.HasPrefix(base, ".") || base == "node_modules") {
				return fs.SkipDir
			}
			return nil
		}
		if strings.HasPrefix(base, ".") {
			return nil
		}
		if info, err := d.Info(); err == nil {
			files[name] = info.ModTime()
		}
		return nil
	})
	return files
}

// firstChange reports a file added, removed, or modified between a and b.
func firstChange(a, b map[string]time.Time) (string, bool) {
	for name, t := range b {
		if old, ok := a[name]; !ok || !old.Equal(t) {
			return name, true
		}
	}
	for name := range a {
		if _, ok := b[name]; !ok {
			return name, true
		}
	}
	return "", false
}

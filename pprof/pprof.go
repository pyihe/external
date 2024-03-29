package gopprof

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/julienschmidt/httprouter"

	"github.com/pyihe/external/concurrency"
)

var server *pprofServer

type pprofServer struct {
	addr       string
	httpServer *http.Server
	router     http.Handler
}

// Start 开启pprof
func Start(addr string) {
	router := httprouter.New()
	router.NotFound = notFoundHandler()
	router.MethodNotAllowed = notAllowedHandler()
	router.PanicHandler = panicHandler()

	server = &pprofServer{
		addr:   addr,
		router: router,
	}

	// /debug/pprof
	router.HandlerFunc("GET", "/debug/pprof/", pprof.Index)

	// /debug/pprof/cmdline
	router.HandlerFunc("GET", "/debug/pprof/cmdline", pprof.Cmdline)

	// /debug/pprof/profile
	router.HandlerFunc("GET", "/debug/pprof/profile", pprof.Profile)

	// GET - /debug/pprof/symbol
	router.HandlerFunc("GET", "/debug/pprof/symbol", pprof.Symbol)

	// POST - /debug/pprof/symbol
	router.HandlerFunc("POST", "/debug/pprof/symbol", pprof.Symbol)

	// /debug/pprof/trace
	router.HandlerFunc("GET", "/debug/pprof/trace", pprof.Trace)

	// /debug/setblockrate
	router.HandlerFunc("PUT", "/debug/setblockrate", setBlockRateHandler())

	// /debug/pprof/heap
	router.Handler("GET", "/debug/pprof/heap", pprof.Handler("heap"))

	// /debug/pprof/goroutine
	router.Handler("GET", "/debug/pprof/goroutine", pprof.Handler("goroutine"))

	// /debug/pprof/allocs
	router.Handler("GET", "/debug/pprof/allocs", pprof.Handler("allocs"))

	// /debug/pprof/block
	router.Handler("GET", "/debug/pprof/block", pprof.Handler("block"))

	// /debug/pprof/threadcreate
	router.Handler("GET", "/debug/pprof/threadcreate", pprof.Handler("threadcreate"))

	// /debug/pprof/mutex
	router.Handler("GET", "/debug/pprof/mutex", pprof.Handler("mutex"))

	concurrency.Go("pprof", server.run)
}

// Stop 终止pprof
func Stop() {
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	server.httpServer.Shutdown(ctx)
	concurrency.Stop("pprof")
}

func (p *pprofServer) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	p.router.ServeHTTP(w, request)
}

func (p *pprofServer) run() {
	ln, err := net.Listen("tcp", p.addr)
	if err != nil {
		slog.Error("start pprof fail: %v", err)
		return
	}

	p.httpServer = &http.Server{
		Handler: p,
	}

	err = p.httpServer.Serve(ln)
	if err != nil && !strings.Contains(err.Error(), "use of closed network connection") &&
		!strings.Contains(err.Error(), "Server closed") {
		slog.Error("pprof serve fail: %v", err)
		return
	}
	slog.Debug("pprof closing: %s", p.addr)
}

func setBlockRateHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, request *http.Request) {
		rate, err := strconv.Atoi(request.FormValue("rate"))
		if err != nil {
			slog.Error("fail to set block rate: %v", err)
			return
		}
		runtime.SetBlockProfileRate(rate)
	}
}

func notFoundHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})
}

func notAllowedHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		w.WriteHeader(http.StatusMethodNotAllowed)
	})
}

func panicHandler() func(w http.ResponseWriter, request *http.Request, p interface{}) {
	return func(w http.ResponseWriter, request *http.Request, p interface{}) {
		w.WriteHeader(http.StatusInternalServerError)
	}
}

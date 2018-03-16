package mgosessionpool

import (
	"bytes"
	"net/http"
	"sync"
)

func (tw *timeoutWriter) setTimedOut() {
	tw.mu.Lock()
	defer tw.mu.Unlock()
	tw.timedOut = true
}

func (tw *timeoutWriter) copyToResponseWriter(w http.ResponseWriter) {
	tw.mu.Lock()
	defer tw.mu.Unlock()
	dst := w.Header()
	for k, vv := range tw.h {
		dst[k] = vv
	}
	if !tw.wroteHeader {
		tw.code = http.StatusOK
	}
	w.WriteHeader(tw.code)
	w.Write(tw.wbuf.Bytes())
}

// NOTE: below is copied from net/http's TimeoutHandler code

// timeoutWriter is borrowed from the net/http package to help prevent data races.
type timeoutWriter struct {
	w    http.ResponseWriter
	h    http.Header
	wbuf bytes.Buffer

	mu          sync.Mutex
	timedOut    bool
	wroteHeader bool
	code        int
}

func (tw *timeoutWriter) Header() http.Header { return tw.h }

func (tw *timeoutWriter) Write(p []byte) (int, error) {
	tw.mu.Lock()
	defer tw.mu.Unlock()
	if tw.timedOut {
		return 0, http.ErrHandlerTimeout
	}
	if !tw.wroteHeader {
		tw.writeHeader(http.StatusOK)
	}
	return tw.wbuf.Write(p)
}

func (tw *timeoutWriter) WriteHeader(code int) {
	tw.mu.Lock()
	defer tw.mu.Unlock()
	if tw.timedOut || tw.wroteHeader {
		return
	}
	tw.writeHeader(code)
}

func (tw *timeoutWriter) writeHeader(code int) {
	tw.wroteHeader = true
	tw.code = code
}

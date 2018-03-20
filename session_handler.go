package mgohttp

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/Clever/mgohttp/internal"
	"gopkg.in/Clever/kayvee-go.v6/logger"
	mgo "gopkg.in/mgo.v2"
)

type mgoSessionKeyType struct {
	database string
}

var mgoSessionKey = mgoSessionKeyType{}

func getMgoSessionKey(db string) mgoSessionKeyType {
	return mgoSessionKeyType{database: db}
}

// SessionHandlerConfig dictates how we inject mongo sessions into the context
// of the HTTP request.
type SessionHandlerConfig struct {
	Sess     *mgo.Session
	Database string
	Timeout  time.Duration
	Handler  http.Handler
}

type mgoSessionCopier interface {
	Copy() *mgo.Session
}

// SessionHandler is an HTTP middleware that injects a new copied mongo session
// into the Context of the request.
// This middleware handles timing out inflight Mongo requests.
type SessionHandler struct {
	parentSession mgoSessionCopier
	database      string
	timeout       time.Duration
	handler       http.Handler
	errorCode     int // this is defaulted to 503, only the tests can override
}

// NewSessionHandler returns a new MongoSessionInjector which implements http.HandlerFunc
func NewSessionHandler(cfg SessionHandlerConfig) http.Handler {
	return &SessionHandler{
		database:      cfg.Database,
		parentSession: cfg.Sess,
		timeout:       cfg.Timeout,
		handler:       cfg.Handler,
		errorCode:     http.StatusServiceUnavailable,
	}
}

// ServeHTTP injects a "getter" to the HTTP request context that allows any wrapped hTTP handler
// to retrieve a new database connection
func (c *SessionHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Instantiate the nil session and timer objects that may be lazily instantiated if
	// the request handler asks for a session.
	var newSession *mgo.Session
	sessionMutex := sync.Mutex{}
	sessionTimer := time.NewTimer(c.timeout)

	// Create a timeoutWriter to avoid races on the http.ResponseWriter.
	tw := &timeoutWriter{
		w: w,
		h: make(http.Header),
	}

	// getSession is injected into the Context, repeated calls by the same request will return
	// the same session.
	var getSession internal.SessionGetter = func() *mgo.Session {
		// we've already created a session for this request, shortcircuit and return that session.
		if newSession != nil {
			return newSession
		}

		sessionMutex.Lock()
		defer sessionMutex.Unlock()
		// Create a session copy. We prefer Copy over Clone because opening new sockets
		// allows for greater throughput to the database.
		// Sessions created using Clone queue all requests through the parent connection's
		// socket. This creates a slow bottleneck when expensive queries appear.
		// NOTE: consider allowing the consumer to pass in a "newSession" function of
		// `func() *mgo.Session` if we are pressed for more flexibility here.
		newSession = c.parentSession.Copy()

		// SetSocketTimeout guarantees that no individual query to mongo can take longer than
		// the RequestTimeoutDuration value.
		newSession.SetSocketTimeout(c.timeout)
		return newSession
	}

	done := make(chan struct{}) // done signifies the end of the HTTP request when closed

	go func() {
		// amend the request context with the database connection then serve the wrapped
		// HTTP handler
		newCtx := internal.NewContext(r.Context(), c.database, getSession)
		c.handler.ServeHTTP(tw, r.WithContext(newCtx))
		close(done)
	}()

	// this select guarantees that we only write to the ResponseWriter a single time
	select {
	case <-done:
		// If we served the request without being preempted by the timer, copy over all the
		// writes from the timeout handler to the actual http.ResponseWriter.
		tw.copyToResponseWriter(w)
	case <-sessionTimer.C:
		tw.setTimedOut()
		w.WriteHeader(c.errorCode)
		logger.FromContext(r.Context()).Error("mongo-session-killed")
	}

	// Finally, if the session exists (which can be true in both the success and timeout case),
	// close the session.
	sessionMutex.Lock()
	defer sessionMutex.Unlock()
	if newSession != nil {
		newSession.Close()
	}
}

// LimitedSession is the methods from an *mgo.Session that we give the consumer access to. We
// omit Close because that is the responsibility of the MongoSessionInjector
type LimitedSession interface {
	DB(string) *mgo.Database
	Ping() error
}

// FromContext retrieves a *mgo.Session from the request context.
func FromContext(ctx context.Context, database string) LimitedSession {
	getSessionBlob := ctx.Value(internal.GetMgoSessionKey(database))
	if getSession, ok := getSessionBlob.(internal.SessionGetter); ok {
		return getSession()
	}

	panic(fmt.Sprintf("SessionFromContext must receive a valid database name: %s not found", database))
}

package mgosessionpool

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/Clever/app-service/config"
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

// MongoSessionInjectorConfig dictates how we inject mongo sessions into the context
// of the HTTP request.
type MongoSessionInjectorConfig struct {
	SessionLimit int64
	Sess         *mgo.Session
	Database     string
	Timeout      time.Duration
	Handler      http.Handler
}

type mgoSessionCopier interface {
	Copy() *mgo.Session
}

// MongoSessionInjector is an HTTP middleware that injects a new copied mongo session
// into the Context of the request.
// This middleware handles timing out inflight Mongo requests.
type MongoSessionInjector struct {
	parentSession mgoSessionCopier
	sessionLimit  int64
	database      string
	timeout       time.Duration
	handler       http.Handler
}

// NewMongoSessionInjector TODO
func NewMongoSessionInjector(cfg MongoSessionInjectorConfig) http.Handler {
	return &MongoSessionInjector{
		database:      cfg.Database,
		parentSession: cfg.Sess,
		timeout:       cfg.Timeout,
		handler:       cfg.Handler,
	}
}

type sessionGetter func() *mgo.Session

func (c *MongoSessionInjector) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Instantiate the nil session and timer objects that may be lazily instantiated if
	// the request handler asks for a session.
	var newSession *mgo.Session
	var sessionTimer *time.Timer

	// getSession is injected into the Context, repeatedly calls by the same request will return
	// the same session.
	var getSession sessionGetter = func() *mgo.Session {
		// we've already created a session for this request, shortcircuit and return that session.
		if newSession != nil {
			return newSession
		}

		// create a session copy
		newSession = c.parentSession.Copy()

		// SetSocketTimeout guarantees that no individual query to mongo can take longer than
		// the RequestTimeoutDuration value.
		newSession.SetSocketTimeout(config.RequestTimeoutDuration)

		// We close the mongo session after RequestTimeoutDuration, this means that no new queries
		// can be opened
		sessionTimer = time.AfterFunc(config.RequestTimeoutDuration, func() {
			logger.FromContext(r.Context()).Error("mongo-session-killed")
			newSession.Close()
		})

		return newSession
	}

	// amend the request context with the database connection then serve the wrapped
	// HTTP handler
	amendedReq := r.WithContext(context.WithValue(r.Context(), getMgoSessionKey(c.database), getSession))
	c.handler.ServeHTTP(w, amendedReq)

	// Try stopping the timer if there is one. If we succeed, close the session
	if sessionTimer != nil && sessionTimer.Stop() {
		logger.FromContext(r.Context()).Info("regular-mongo-session-close")
		newSession.Close()
	}
}

// LimitedSession is the methods from an *mgo.Session that we give the consumer access to. We
// omit Close because that is the responsibility of the MongoSessionInjector
type LimitedSession interface {
	DB(string) *mgo.Database
	Ping() error
}

// SessionFromContext retrieves a *mgo.Session from the request context.
func SessionFromContext(ctx context.Context, database string) LimitedSession {
	getSessionBlob := ctx.Value(getMgoSessionKey(database))
	if getSession, ok := getSessionBlob.(sessionGetter); ok {
		return getSession()
	}

	panic(fmt.Sprintf("SessionFromContext must receive a valid database name: %s not found", database))
}

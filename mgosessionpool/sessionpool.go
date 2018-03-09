package mgosessionpool

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/Clever/app-service/config"
	"golang.org/x/sync/semaphore"
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

// MongoSessionInjector is an HTTP middleware that injects a new copied mongo session
// into the Context of the request.
// This middleware handles timing out inflight Mongo requests.
type MongoSessionInjector struct {
	parentSession *mgo.Session
	sessionLimit  int64
	database      string
	timeout       time.Duration
	handler       http.Handler
	sem           *semaphore.Weighted
}

// NewMongoSessionInjector TODO
func NewMongoSessionInjector(cfg MongoSessionInjectorConfig) http.Handler {
	return &MongoSessionInjector{
		database:      cfg.Database,
		parentSession: cfg.Sess,
		timeout:       cfg.Timeout,
		handler:       cfg.Handler,
		sem:           semaphore.NewWeighted(cfg.SessionLimit),
	}
}

func (c *MongoSessionInjector) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	if err := c.sem.Acquire(ctx, 1); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprintf("mongo pool exhausted at %d conns", c.sessionLimit)))
		return
	}
	defer c.sem.Release(1)

	// create a session copy
	newSession := c.parentSession.Copy()

	// SetSocketTimeout guarantees that no individual query to mongo can take longer than
	// the RequestTimeoutDuration value.
	newSession.SetSocketTimeout(config.RequestTimeoutDuration)

	// We close the mongo session after RequestTimeoutDuration, this means that no new queries
	// can be opened
	timer := time.AfterFunc(config.RequestTimeoutDuration, func() {
		logger.FromContext(r.Context()).Error("mongo-session-killed")
		newSession.Close()
	})

	// amend the request context with the database connection then serve the wrapped
	// HTTP handler
	r = r.WithContext(context.WithValue(r.Context(), getMgoSessionKey(c.database), newSession))
	c.handler.ServeHTTP(w, r)

	// Try stopping the timer. If we succeed, close the session
	if timer.Stop() {
		logger.FromContext(r.Context()).Info("regular-mongo-session-close")
		newSession.Close()
	}
}

// LimitedSession is the methods from an *mgo.Session that we give the consumer access to
type LimitedSession interface {
	DB(string) *mgo.Database
	Ping() error
}

// SessionFromContext retrieves a *mgo.Session from the request context.
func SessionFromContext(ctx context.Context, database string) LimitedSession {
	sessionBlob := ctx.Value(getMgoSessionKey(database))
	if sess, ok := sessionBlob.(*mgo.Session); ok {
		return sess
	}
	panic(fmt.Sprintf("SessionFromContext must receive a valid database name: %s not found", database))
}

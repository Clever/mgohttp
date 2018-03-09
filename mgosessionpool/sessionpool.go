package mgosessionpool

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/Clever/app-service/config"
	"github.com/Clever/kayvee-go/logger"
	"golang.org/x/sync/semaphore"
	mgo "gopkg.in/mgo.v2"
)

// Session TODO
type Session interface {
	CopyWithCtx(ctx context.Context) (Session, error)
	Close()
	SetSocketTimeout(time.Duration)
	DB(name string) *mgo.Database
	Ping() error
}

// SessionPool is a session with limits on how many concurrent copies can exist.
type SessionPool struct {
	session  *mgo.Session
	sem      *semaphore.Weighted
	copied   bool
	released bool
}

// NewSessionPool creates a new session pool.
func NewSessionPool(session *mgo.Session, limit int64) *SessionPool {
	return &SessionPool{
		session: session,
		sem:     semaphore.NewWeighted(limit),
		copied:  false,
	}
}

// CopyWithCtx creates a new session copy.
func (s *SessionPool) CopyWithCtx(ctx context.Context) (Session, error) {
	// the underlying mgo session can only be copied once. This protects against
	// deadlock in the case where a function has a nested call to another session copy'er.
	// it assumes one session copy per request is sufficient
	if s.copied {
		return s, nil
	}
	if err := s.sem.Acquire(ctx, 1); err != nil {
		return nil, err
	}
	return &SessionPool{
		session: s.session.Copy(),
		sem:     s.sem,
		copied:  true,
	}, nil
}

// DB returns a mongo DB.
func (s *SessionPool) DB(name string) *mgo.Database {
	return s.session.DB(name)
}

// SetSocketTimeout sets the timeout for queries from this session
func (s *SessionPool) SetSocketTimeout(d time.Duration) {
	s.session.SetSocketTimeout(d)
}

// Ping mongo.
func (s *SessionPool) Ping() error {
	return s.session.Ping()
}

// Close the session.
func (s *SessionPool) Close() {
	if s.copied && !s.released {
		// lock was acquired, release it
		s.released = true
		s.sem.Release(1)
	}
	s.session.Close()
}

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
	SessionPool *SessionPool
	Database    string
	Timeout     time.Duration
	Handler     http.Handler
}

// MongoSessionInjector is an HTTP middleware that injects a new copied mongo session
// into the Context of the request.
// This middleware handles timing out inflight Mongo requests.
type MongoSessionInjector struct {
	sessionPool *SessionPool
	database    string
	timeout     time.Duration
	handler     http.Handler
}

// NewMongoSessionInjector TODO
func NewMongoSessionInjector(cfg MongoSessionInjectorConfig) http.Handler {
	return &MongoSessionInjector{
		database:    cfg.Database,
		sessionPool: cfg.SessionPool,
		timeout:     cfg.Timeout,
		handler:     cfg.Handler,
	}
}

func (c *MongoSessionInjector) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// create a session copy
	newSession, err := c.sessionPool.CopyWithCtx(r.Context())
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

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

type LimitedSession interface {
	DB(string) *mgo.Database
}

func SessionFromContext(ctx context.Context, database string) LimitedSession {
	sessionBlob := ctx.Value(getMgoSessionKey(database))
	if sess, ok := sessionBlob.(*SessionPool); ok {
		return sess
	}
	panic(fmt.Sprintf("SessionFromContext must receive a valid database name: %s not found", database))
}

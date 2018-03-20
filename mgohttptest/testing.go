package mgohttptest

import (
	"context"
	"sync"

	"github.com/Clever/mgohttp/internal"
	mgo "gopkg.in/mgo.v2"
)

// InjectSession returns a new session copy.
func InjectSession(ctx context.Context, parentSession *mgo.Session, dbName string) context.Context {
	var getSession internal.SessionGetter = func() *mgo.Session {
		// We create a new session on every call. This should be fine for testing. This
		// would not be OK for production which is why we isolated it into mgohttptest.
		return parentSession.Copy()
	}
	return context.WithValue(ctx, internal.GetMgoSessionKey(dbName), getSession)
}

// Config describes a mongo database that will be injected to the context
type Config struct {
	Name string
	Sess *mgo.Session
}

// WithSessions calls a provided function with every provided database config injected to the
func WithSessions(cfgs []Config, Do func(context.Context)) {
	ctx := context.TODO()

	sessionMu := sync.Mutex{}
	sessions := []*mgo.Session{}

	for _, c := range cfgs {
		var getSession internal.SessionGetter = func() *mgo.Session {
			// We track all sessions created so that we can close them at the end of the test
			newSess := c.Sess.Copy()
			sessionMu.Lock()
			defer sessionMu.Unlock()
			sessions = append(sessions, newSess)
			return newSess
		}
		ctx = context.WithValue(ctx, internal.GetMgoSessionKey(c.Name), getSession)
	}

	Do(ctx)

	sessionMu.Lock()
	defer sessionMu.Unlock()
	for _, c := range sessions {
		c.Close()
	}
}

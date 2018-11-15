package mgohttptest

import (
	"context"

	"github.com/Clever/mgohttp/internal"
	mgo "github.com/globalsign/mgo"
)

// Config describes a mongo database that will be injected to the context
type Config struct {
	Name string
	Sess *mgo.Session
}

// DbHandler manages our interaction with the testing Context.
type DbHandler interface {
	context.Context
	Close()
}

// testContext embeds a context and tracks open sessions so then can be cleared out on Close.
type testContext struct {
	context.Context
	sessions []*mgo.Session
}

// Close calls Close on all tracked *mgo.Session's
func (t testContext) Close() {
	for _, s := range t.sessions {
		s.Close()
	}
}

// MakeContext creates a new Context that contains mgohttp database connections.
func MakeContext(ctx context.Context, cfgs ...Config) DbHandler {
	// We track all sessions created so that we can close them
	sessions := []*mgo.Session{}

	for _, c := range cfgs {
		newSess := c.Sess.Copy()
		sessions = append(sessions, newSess)
		var getSession internal.SessionGetter = func() *mgo.Session {
			return newSess
		}
		ctx = internal.NewContext(ctx, c.Name, getSession)
	}

	return testContext{
		Context:  ctx,
		sessions: sessions,
	}
}

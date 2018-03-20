// Package internal exists so that we can share the Context key functions between
// mgohttp & mgohttptest without making them public.
package internal

import (
	"context"

	mgo "gopkg.in/mgo.v2"
)

type mgoSessionKeyType struct {
	database string
}

var mgoSessionKey = mgoSessionKeyType{}

//GetMgoSessionKey returns a new object for use as a Context object key.
func GetMgoSessionKey(db string) interface{} {
	return mgoSessionKeyType{database: db}
}

// SessionGetter is the function type definition used to enforce that we're populating the
// Context value with the correct function type.
type SessionGetter func() *mgo.Session

// NewContext creates a new context object containing a new mgo session getter.
func NewContext(ctx context.Context, dbName string, getter SessionGetter) context.Context {
	return context.WithValue(ctx, GetMgoSessionKey(dbName), getter)
}

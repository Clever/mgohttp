package mgohttp

import (
	mgo "gopkg.in/mgo.v2"
)

// MongoSession wraps a subset of the session interface to Mongo for tracing purposes
type MongoSession interface {
	DB(name string) MongoDatabase
	Ping() error
}

// MongoDatabase wraps a subset of the Database interface to Mongo for tracing purposes
type MongoDatabase interface {
	C(collection string) MongoCollection
	Run(cmd interface{}, result interface{}) error
}

// MongoCollection wraps a subset of the Collection interface to Mongo for tracing purposes
type MongoCollection interface {
	Update(selector interface{}, update interface{}) error
	Upsert(selector interface{}, update interface{}) (info *mgo.ChangeInfo, err error)
	Find(query interface{}) MongoQuery
	Remove(selector interface{}) error
	RemoveAll(selector interface{}) (info *mgo.ChangeInfo, err error)
}

// MongoQuery wraps a subset of the Query interface to Mongo for tracing purposes
type MongoQuery interface {
	All(result interface{}) error
	One(result interface{}) (err error)
	Limit(n int) MongoQuery
	Select(selector interface{}) MongoQuery
	Hint(indexKey ...string) MongoQuery
	Sort(fields ...string) MongoQuery
}

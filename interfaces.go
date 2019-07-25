package mgohttp

import (
	mgo "gopkg.in/mgo.v2"
	bson "gopkg.in/mgo.v2/bson"
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
	Find(query interface{}) MongoQuery
	FindId(id bson.ObjectId) MongoQuery
	Insert(docs ...interface{}) error
	Remove(selector interface{}) error
	RemoveId(id bson.ObjectId) error
	RemoveAll(selector interface{}) (info *mgo.ChangeInfo, err error)
	Update(selector interface{}, update interface{}) error
	UpdateId(id bson.ObjectId, update interface{}) error
	UpdateAll(selector interface{}, update interface{}) (info *mgo.ChangeInfo, err error)
	Upsert(selector interface{}, update interface{}) (info *mgo.ChangeInfo, err error)
}

// MongoQuery wraps a subset of the Query interface to Mongo for tracing purposes
type MongoQuery interface {
	All(result interface{}) error
	Apply(change mgo.Change, result interface{}) (info *mgo.ChangeInfo, err error)
	Hint(indexKey ...string) MongoQuery
	Iter() MongoIter
	Limit(n int) MongoQuery
	One(result interface{}) (err error)
	Select(selector interface{}) MongoQuery
	Sort(fields ...string) MongoQuery
}

// MongoIter wraps the non-deprecated methods of an `mgo.Iter` for tracing purposes
type MongoIter interface {
	All(result interface{}) error
	Close() error
	Done() bool
	Err() error
	Next(result interface{}) bool
}

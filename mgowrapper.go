package mgohttp

import (
	"context"
	"fmt"
	"strings"

	opentracing "github.com/opentracing/opentracing-go"
	opentracinglog "github.com/opentracing/opentracing-go/log"
	mgo "gopkg.in/mgo.v2"
	bson "gopkg.in/mgo.v2/bson"
)

type tracedMgoSession struct {
	sess *mgo.Session
	ctx  context.Context
}

func (ts tracedMgoSession) DB(name string) MongoDatabase {
	sp := opentracing.SpanFromContext(ts.ctx)
	sp.SetTag("db-name", name)
	return tracedMgoDatabase{
		db:  ts.sess.DB(name),
		ctx: opentracing.ContextWithSpan(ts.ctx, sp),
	}
}

func (ts tracedMgoSession) Ping() error {
	sp, _ := opentracing.StartSpanFromContext(ts.ctx, "ping")
	defer sp.Finish()

	return logAndReturnErr(sp, ts.sess.Ping())
}

type tracedMgoDatabase struct {
	db  *mgo.Database
	ctx context.Context
}

func (t tracedMgoDatabase) C(collection string) MongoCollection {
	return tracedMgoCollection{
		collectionName: collection,
		collection:     t.db.C(collection),
		ctx:            t.ctx,
	}
}

func (t tracedMgoDatabase) Run(cmd interface{}, result interface{}) error {
	sp, _ := opentracing.StartSpanFromContext(t.ctx, "run")
	defer sp.Finish()
	sp.LogKV(opentracinglog.String("cmd", fmt.Sprintf("%#v", cmd)))

	return logAndReturnErr(sp, t.db.Run(cmd, result))
}

type tracedMgoCollection struct {
	collectionName string
	collection     *mgo.Collection
	ctx            context.Context
}

func (tc tracedMgoCollection) UpdateId(id bson.ObjectId, update interface{}) error {
	return tc.Update(bson.M{"_id": id}, update)
}

func (tc tracedMgoCollection) Update(selector interface{}, update interface{}) error {
	sp, _ := opentracing.StartSpanFromContext(tc.ctx, "update")
	sp.SetTag("collection", tc.collectionName)
	sp.LogFields(bsonToKeys("selector", selector))
	sp.LogFields(bsonToKeys("update", update))
	defer sp.Finish()

	return logAndReturnErr(sp, tc.collection.Update(selector, update))
}

func (tc tracedMgoCollection) UpdateAll(selector interface{}, update interface{}) (info *mgo.ChangeInfo, err error) {
	sp, _ := opentracing.StartSpanFromContext(tc.ctx, "update-all")
	sp.SetTag("collection", tc.collectionName)
	sp.LogFields(bsonToKeys("selector", selector))
	sp.LogFields(bsonToKeys("update", update))
	defer sp.Finish()

	info, err = tc.collection.UpdateAll(selector, update)
	return info, logAndReturnErr(sp, err)
}

func (tc tracedMgoCollection) Insert(docs ...interface{}) (err error) {
	sp, _ := opentracing.StartSpanFromContext(tc.ctx, "insert")
	sp.LogFields(opentracinglog.Int("num-docs", len(docs)))
	defer sp.Finish()

	return logAndReturnErr(sp, tc.collection.Insert(docs...))
}

func (tc tracedMgoCollection) Upsert(selector interface{}, update interface{}) (info *mgo.ChangeInfo, err error) {
	sp, _ := opentracing.StartSpanFromContext(tc.ctx, "upsert")
	sp.LogFields(bsonToKeys("selector", selector))
	sp.LogFields(bsonToKeys("update", update))
	defer sp.Finish()

	info, err = tc.collection.Upsert(selector, update)
	return info, logAndReturnErr(sp, err)
}

func (tc tracedMgoCollection) FindId(id bson.ObjectId) MongoQuery {
	return tc.Find(bson.M{"_id": id})
}

func (tc tracedMgoCollection) Find(selector interface{}) MongoQuery {
	sp, ctx := opentracing.StartSpanFromContext(tc.ctx, "find")
	sp.SetTag("collection", tc.collectionName)

	// NOTE: Find just starts the trace, the finishing call on the MongoQuery must
	// finish it.
	sp.LogFields(bsonToKeys("selector", selector))
	return tracedMongoQuery{
		q:   tc.collection.Find(selector),
		ctx: ctx,
	}
}

func (tc tracedMgoCollection) RemoveId(id bson.ObjectId) error {
	return tc.Remove(bson.M{"_id": id})
}

func (tc tracedMgoCollection) Remove(selector interface{}) error {
	sp, _ := opentracing.StartSpanFromContext(tc.ctx, "remove")
	sp.SetTag("collection", tc.collectionName)
	sp.LogFields(bsonToKeys("selector", selector))
	defer sp.Finish()

	return logAndReturnErr(sp, tc.collection.Remove(selector))
}

func (tc tracedMgoCollection) RemoveAll(selector interface{}) (info *mgo.ChangeInfo, err error) {
	sp, _ := opentracing.StartSpanFromContext(tc.ctx, "removeall")
	sp.SetTag("collection", tc.collectionName)
	sp.LogFields(bsonToKeys("selector", selector))
	defer sp.Finish()

	info, err = tc.collection.RemoveAll(selector)
	return info, logAndReturnErr(sp, err)
}

type tracedMongoQuery struct {
	q   *mgo.Query
	ctx context.Context
}

func (q tracedMongoQuery) All(result interface{}) error {
	sp := opentracing.SpanFromContext(q.ctx)
	defer sp.Finish()

	sp.SetTag("access-method", "All")
	return logAndReturnErr(sp, q.q.All(result))
}

func (q tracedMongoQuery) One(result interface{}) (err error) {
	sp := opentracing.SpanFromContext(q.ctx)
	defer sp.Finish()

	sp.SetTag("access-method", "One")
	return logAndReturnErr(sp, q.q.One(result))
}

func (q tracedMongoQuery) Count() (int, error) {
	sp := opentracing.SpanFromContext(q.ctx)
	defer sp.Finish()

	sp.SetTag("access-method", "Count")
	n, err := q.q.Count()
	return n, logAndReturnErr(sp, err)
}

func (q tracedMongoQuery) Limit(n int) MongoQuery {
	// NOTE: this function just modifies the query, we will rely on
	// One/All to terminate the span.

	sp := opentracing.SpanFromContext(q.ctx)
	sp.LogFields(opentracinglog.Int("query-limit", n))
	return tracedMongoQuery{
		q:   q.q.Limit(n),
		ctx: opentracing.ContextWithSpan(q.ctx, sp),
	}
}

func (q tracedMongoQuery) Select(selector interface{}) MongoQuery {
	// NOTE: this function just modifies the query, we will rely on
	// One/All to terminate the span.

	sp := opentracing.SpanFromContext(q.ctx)
	sp.LogFields(bsonToKeys("select", selector))
	return tracedMongoQuery{
		q:   q.q.Select(selector),
		ctx: opentracing.ContextWithSpan(q.ctx, sp),
	}
}

func (q tracedMongoQuery) Hint(indexKey ...string) MongoQuery {
	// NOTE: this function just modifies the query, we will rely on
	// One/All to terminate the span.

	sp := opentracing.SpanFromContext(q.ctx)
	for i, hint := range indexKey {
		sp.LogFields(opentracinglog.String(fmt.Sprintf("hint.%d", i), hint))
	}

	return tracedMongoQuery{
		q:   q.q.Hint(indexKey...),
		ctx: opentracing.ContextWithSpan(q.ctx, sp),
	}
}

func (q tracedMongoQuery) Sort(fields ...string) MongoQuery {
	// NOTE: this function just modifies the query, we will rely on
	// One/All to terminate the span.

	sp := opentracing.SpanFromContext(q.ctx)
	sp.SetTag("sort", strings.Join(fields, "|"))
	return tracedMongoQuery{
		q:   q.q.Sort(fields...),
		ctx: opentracing.ContextWithSpan(q.ctx, sp),
	}
}

func (q tracedMongoQuery) Apply(change mgo.Change, result interface{}) (info *mgo.ChangeInfo, err error) {
	sp := opentracing.SpanFromContext(q.ctx)
	defer sp.Finish()

	sp.SetTag("access-method", "apply")
	sp.LogFields(bsonToKeys("update", change.Update))
	sp.LogFields(
		opentracinglog.Bool("remove", change.Remove),
		opentracinglog.Bool("return-new", change.ReturnNew),
		opentracinglog.Bool("upsert", change.Upsert),
	)

	info, err = q.q.Apply(change, result)
	return info, logAndReturnErr(sp, err)
}

func (q tracedMongoQuery) Iter() MongoIter {
	_, ctx := opentracing.StartSpanFromContext(q.ctx, "iter")
	return tracedMongoIter{
		i:   q.q.Iter(),
		ctx: ctx,
	}
}

type tracedMongoIter struct {
	i   *mgo.Iter
	ctx context.Context
}

func (t tracedMongoIter) All(result interface{}) error {
	sp, _ := opentracing.StartSpanFromContext(t.ctx, "iter-all")
	defer sp.Finish()
	return logAndReturnErr(sp, t.i.All(result))
}

func (t tracedMongoIter) Close() error {
	sp := opentracing.SpanFromContext(t.ctx)
	defer sp.Finish()
	return logAndReturnErr(sp, t.i.Close())
}

func (t tracedMongoIter) Done() bool {
	return t.i.Done()

}
func (t tracedMongoIter) Err() error {
	return logAndReturnErr(opentracing.SpanFromContext(t.ctx), t.i.Err())
}

func (t tracedMongoIter) Next(result interface{}) bool {
	sp, _ := opentracing.StartSpanFromContext(t.ctx, "iter-next")
	defer sp.Finish()
	return t.i.Next(result)
}

// logAndReturnErr is a tiny helper for adding the error to a log inline.
func logAndReturnErr(sp opentracing.Span, err error) error {
	sp.LogFields(opentracinglog.Error(err))
	return err
}

func getKeys(prefix string, q bson.M) []string {
	addPrefix := func(s string) string {
		if prefix == "" {
			return s
		}
		return prefix + "." + s
	}

	fields := []string{}
	for k, v := range q {
		switch val := v.(type) {
		case bson.M:
			fields = append(fields, getKeys(addPrefix(k), val)...)
		default:
			fields = append(fields, addPrefix(k))
		}
	}
	return fields
}

// bsonToKeys transforms an arbitrary mgo arg into a set of log fields.
// This is mostly geared towards bson.M, but the Sprintf fallback should handle arrays
// sufficiently for tracing purposes.
func bsonToKeys(name string, query interface{}) opentracinglog.Field {
	queryFields := []string{}
	if q, ok := query.(bson.M); ok {
		queryFields = getKeys("", q)
	}
	return opentracinglog.String(name, strings.Join(queryFields, "|"))
}

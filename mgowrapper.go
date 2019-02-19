package mgohttp

import (
	"context"
	"fmt"

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
	sp.LogFields(opentracinglog.String("mgo-db-name", name))
	return tracedMgoDatabase{
		db:  ts.sess.DB(name),
		ctx: ts.ctx,
	}
}

func (ts tracedMgoSession) Ping() error {
	sp, _ := opentracing.StartSpanFromContext(ts.ctx, "ping")
	defer sp.Finish()

	err := ts.sess.Ping()
	sp.LogFields(opentracinglog.Error(err))
	return err
}

type tracedMgoDatabase struct {
	db  *mgo.Database
	ctx context.Context
}

func (t tracedMgoDatabase) C(collection string) MongoCollection {
	sp := opentracing.SpanFromContext(t.ctx)
	sp.LogFields(opentracinglog.String("mgo-collection-name", collection))

	return tracedMgoCollection{
		collection: t.db.C(collection),
		ctx:        t.ctx,
	}
}

func (t tracedMgoDatabase) Run(cmd interface{}, result interface{}) error {
	sp, _ := opentracing.StartSpanFromContext(t.ctx, "run")
	defer sp.Finish()
	sp.LogKV(opentracinglog.String("cmd", fmt.Sprintf("%#v", cmd)))

	return logAndReturnErr(sp, t.db.Run(cmd, result))
}

type tracedMgoCollection struct {
	collection *mgo.Collection
	ctx        context.Context
}

func (tc tracedMgoCollection) Update(selector interface{}, update interface{}) error {
	sp, _ := opentracing.StartSpanFromContext(tc.ctx, "update")
	sp.LogFields(queryToFields("selector", selector)...)
	sp.LogFields(queryToFields("update", update)...)
	defer sp.Finish()

	return logAndReturnErr(sp, tc.collection.Update(selector, update))
}

func (tc tracedMgoCollection) Upsert(selector interface{}, update interface{}) (info *mgo.ChangeInfo, err error) {
	sp, _ := opentracing.StartSpanFromContext(tc.ctx, "upsert")
	sp.LogFields(queryToFields("selector", selector)...)
	sp.LogFields(queryToFields("update", update)...)
	defer sp.Finish()

	info, err = tc.Upsert(selector, update)
	return info, logAndReturnErr(sp, err)
}

func (tc tracedMgoCollection) Find(selector interface{}) MongoQuery {
	sp, ctx := opentracing.StartSpanFromContext(tc.ctx, "find")
	// NOTE: Find just starts the trace, the finishing call on the MongoQuery must
	// finish it.
	sp.LogFields(queryToFields("selector", selector)...)
	return tracedMongoQuery{
		q:   tc.collection.Find(selector),
		ctx: ctx,
	}
}

func (tc tracedMgoCollection) Remove(selector interface{}) error {
	sp, _ := opentracing.StartSpanFromContext(tc.ctx, "remove")
	sp.LogFields(queryToFields("selector", selector)...)
	defer sp.Finish()

	return logAndReturnErr(sp, tc.collection.Remove(selector))
}

type tracedMongoQuery struct {
	q   *mgo.Query
	ctx context.Context
}

func (q tracedMongoQuery) All(result interface{}) error {
	sp := opentracing.SpanFromContext(q.ctx)
	defer sp.Finish()
	sp.LogFields(opentracinglog.String("access-method", "All"))
	return logAndReturnErr(sp, q.q.All(result))
}

func (q tracedMongoQuery) One(result interface{}) (err error) {
	sp := opentracing.SpanFromContext(q.ctx)
	defer sp.Finish()
	sp.LogFields(opentracinglog.String("access-method", "One"))
	return logAndReturnErr(sp, q.q.One(result))
}
func (q tracedMongoQuery) Limit(n int) MongoQuery {
	sp := opentracing.SpanFromContext(q.ctx)
	sp.LogFields(opentracinglog.Int("query-limit", n))
	return tracedMongoQuery{
		q:   q.q.Limit(n),
		ctx: q.ctx,
	}
}

func (q tracedMongoQuery) Select(selector interface{}) MongoQuery {
	sp := opentracing.SpanFromContext(q.ctx)
	sp.LogFields(queryToFields("select", selector)...)
	return tracedMongoQuery{
		q:   q.q.Select(selector),
		ctx: q.ctx,
	}
}

func (q tracedMongoQuery) Hint(indexKey ...string) MongoQuery {
	sp := opentracing.SpanFromContext(q.ctx)
	for i, hint := range indexKey {
		sp.LogFields(opentracinglog.String(fmt.Sprintf("hint.%d", i), hint))
	}

	return tracedMongoQuery{
		q:   q.q.Hint(indexKey...),
		ctx: q.ctx,
	}
}

func (q tracedMongoQuery) Sort(fields ...string) MongoQuery {
	sp := opentracing.SpanFromContext(q.ctx)
	sp.LogFields(queryToFields("sort", fields)...)
	return tracedMongoQuery{
		q:   q.q.Sort(fields...),
		ctx: q.ctx,
	}
}

// logAndReturnErr is a tiny helper for adding the error to a log inline.
func logAndReturnErr(sp opentracing.Span, err error) error {
	sp.LogFields(opentracinglog.Error(err))
	return err
}

// queryToFields transforms an arbitrary mgo arg into a set of log fields.
// This is mostly geared towards bson.M, but the Sprintf fallback should handle arrays
// sufficiently for tracing purposes.
func queryToFields(prefix string, query interface{}) []opentracinglog.Field {
	fields := []opentracinglog.Field{}
	switch q := query.(type) {
	case bson.M:
		for k, v := range q {
			k = fmt.Sprintf("%s.%s", prefix, k)

			switch val := v.(type) {
			case string:
				fields = append(fields, opentracinglog.String(k, val))
			case int:
				fields = append(fields, opentracinglog.Int(k, val))
			case bson.ObjectId:
				fields = append(fields, opentracinglog.String(k, val.Hex()))
			case bson.M:
				fields = append(fields, opentracinglog.Object(k, val))
			default:
				fields = append(fields, opentracinglog.String(k, fmt.Sprintf("%#v", val)))
			}
		}
		return fields
	default:
		return append(fields, opentracinglog.String(prefix, fmt.Sprintf("%#v", query)))
	}
}

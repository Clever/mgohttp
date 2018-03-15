package mgosessionpool

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

const (
	testMongoURL   = "127.0.0.1:27017"
	testDBName     = "mgosessionpool-test"
	handlerTimeout = 50 * time.Millisecond
)

func TestMongoSessionInjector(t *testing.T) {
	session, err := mgo.Dial(testMongoURL + "/mgosessionpool-test")
	require.NoError(t, err)
	defer session.Close()

	testCases := []struct {
		desc       string
		handler    http.HandlerFunc
		assertions func(*testing.T, *http.Response)
	}{
		{
			desc: "simple ping twice",
			handler: func(w http.ResponseWriter, r *http.Request) {
				sess := SessionFromContext(r.Context(), testDBName)
				if sess.Ping() != nil {
					w.WriteHeader(http.StatusInternalServerError)
					return
				}

				sess2 := SessionFromContext(r.Context(), testDBName)
				if sess2.Ping() != nil {
					w.WriteHeader(http.StatusInternalServerError)
					return
				}

				w.WriteHeader(http.StatusOK)
			},
			assertions: func(t *testing.T, resp *http.Response) {
				assert.Equal(t, http.StatusOK, resp.StatusCode)
			},
		},
		{
			desc: "endpoint timeout for single query",
			handler: func(w http.ResponseWriter, r *http.Request) {
				sess := SessionFromContext(r.Context(), testDBName)
				// try to sleep for 10sec
				err := sess.DB("test").Run(bson.M{"eval": "sleep(10000)"}, nil)
				if err != nil {
					// NOTE: using 500 to differentiate from the injector's 503's
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				// this should not be reached
				w.WriteHeader(http.StatusOK)
			},
			assertions: func(t *testing.T, resp *http.Response) {
				assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
			},
		},
		{
			desc: "endpoint timeout with many queries",
			handler: func(w http.ResponseWriter, r *http.Request) {
				// try to small query many times
				for i := 0; i < 1000; i++ {
					sess := SessionFromContext(r.Context(), testDBName)
					err := sess.DB("test").Run(bson.M{"eval": "sleep(10)"}, nil)
					if err != nil {
						// NOTE: using 500 to differentiate from the injector's 503's
						w.WriteHeader(http.StatusInternalServerError)
						return
					}
				}
				// this should not be reached
				w.WriteHeader(http.StatusOK)
			},
			assertions: func(t *testing.T, resp *http.Response) {
				assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
			},
		},
	}

	for _, spec := range testCases {
		t.Run(spec.desc, func(t *testing.T) {
			injector := NewMongoSessionInjector(MongoSessionInjectorConfig{
				Sess:     session,
				Database: testDBName,
				Timeout:  handlerTimeout,
				Handler:  spec.handler,
			})

			testServer := httptest.NewServer(injector)
			defer testServer.Close()

			resp, err := http.Get(testServer.URL)
			require.NoError(t, err)
			spec.assertions(t, resp)
		})
	}
}

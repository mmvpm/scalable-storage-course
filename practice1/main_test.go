package main

import (
	"bytes"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geojson"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

func TestSimple(t *testing.T) {
	mux := http.NewServeMux()

	storage := NewStorage(mux, "test", []string{}, true, "test.json")
	router := NewRouter(mux, [][]string{{"test"}}, "../front/dist")

	go storage.Run()
	go router.Run()

	t.Cleanup(router.Stop)
	t.Cleanup(storage.Stop)
	t.Cleanup(func() { _ = os.Remove("test.json") })

	feature := newFeatureWithID(orb.Point{rand.Float64(), rand.Float64()}, "a15d5061-999e-4168-9b58-7b508a2dadaf")

	body, err := feature.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}

	req, err := http.NewRequest("POST", "/insert", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code == http.StatusTemporaryRedirect {
		req, err := http.NewRequest("POST", rr.Header().Get("location"), bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		rr := httptest.NewRecorder()

		mux.ServeHTTP(rr, req)

		if rr.Code != http.StatusOK {
			t.Errorf("handler returned wrong status code: got %v want %v", rr.Code, http.StatusOK)
		}
	} else if rr.Code != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v", rr.Code, http.StatusOK)
	}
}

func TestGet(t *testing.T) {
	mux := http.NewServeMux()

	storage := NewStorage(mux, "test", []string{}, true, "test.json")
	router := NewRouter(mux, [][]string{{"test"}}, "../front/dist")

	go storage.Run()
	go router.Run()

	t.Cleanup(router.Stop)
	t.Cleanup(storage.Stop)
	t.Cleanup(func() { _ = os.Remove("test.json") })

	rr := httptest.NewRecorder()

	// prepare db
	existingFeature := newFeatureWithID(orb.Point{rand.Float64(), rand.Float64()}, "existing-id")
	insert(t, existingFeature, mux, rr)

	req, err := http.NewRequest("GET", "/select", nil)
	if err != nil {
		t.Fatal(err)
	}

	mux.ServeHTTP(rr, req)

	if rr.Code == http.StatusTemporaryRedirect {
		req, err := http.NewRequest("GET", rr.Header().Get("location"), nil)
		if err != nil {
			t.Fatal(err)
		}
		rr := httptest.NewRecorder()

		mux.ServeHTTP(rr, req)

		if rr.Code != http.StatusOK {
			t.Errorf("handler returned wrong status code: got %v want %v", rr.Code, http.StatusOK)
		}
	} else if rr.Code != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v", rr.Code, http.StatusOK)
	}
}

func TestInsert(t *testing.T) {
	mux := http.NewServeMux()

	storage := NewStorage(mux, "test", []string{}, true, "test.json")
	router := NewRouter(mux, [][]string{{"test"}}, "../front/dist")

	go storage.Run()
	go router.Run()

	t.Cleanup(router.Stop)
	t.Cleanup(storage.Stop)
	t.Cleanup(func() { _ = os.Remove("test.json") })

	tests := []struct {
		name     string
		feature  *geojson.Feature
		wantCode int
	}{
		{
			name:     "Valid Insert",
			feature:  newFeatureWithID(orb.Point{rand.Float64(), rand.Float64()}, "a15d5061-999e-4168-9b58-7b508a2dadaf"),
			wantCode: http.StatusOK,
		},
		{
			name:     "Insert Without ID",
			feature:  geojson.NewFeature(orb.Point{rand.Float64(), rand.Float64()}),
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "Insert Duplicate ID",
			feature:  newFeatureWithID(orb.Point{rand.Float64(), rand.Float64()}, "a15d5061-999e-4168-9b58-7b508a2dadaf"),
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body, err := tt.feature.MarshalJSON()
			if err != nil {
				t.Fatal(err)
			}

			req, err := http.NewRequest("POST", "/insert", bytes.NewReader(body))
			if err != nil {
				t.Fatal(err)
			}

			rr := httptest.NewRecorder()
			mux.ServeHTTP(rr, req)

			if rr.Code == http.StatusTemporaryRedirect {
				req, err := http.NewRequest("POST", rr.Header().Get("location"), bytes.NewReader(body))
				if err != nil {
					t.Fatal(err)
				}
				rr := httptest.NewRecorder()

				mux.ServeHTTP(rr, req)

				if rr.Code != tt.wantCode {
					t.Errorf("handler returned wrong status code: got %v want %v", rr.Code, tt.wantCode)
				}
			} else if rr.Code != http.StatusOK {
				t.Errorf("handler returned wrong status code: got %v want %v", rr.Code, http.StatusOK)
			}
		})
	}
}

func TestReplace(t *testing.T) {
	mux := http.NewServeMux()

	storage := NewStorage(mux, "test", []string{}, true, "test.json")
	router := NewRouter(mux, [][]string{{"test"}}, "../front/dist")

	go storage.Run()
	go router.Run()

	t.Cleanup(router.Stop)
	t.Cleanup(storage.Stop)
	t.Cleanup(func() { _ = os.Remove("test.json") })

	tests := []struct {
		name     string
		feature  *geojson.Feature
		wantCode int
	}{
		{
			name:     "Valid Replace",
			feature:  newFeatureWithID(orb.Point{rand.Float64(), rand.Float64()}, "existing-id"),
			wantCode: http.StatusOK,
		},
		{
			name:     "Replace Without ID",
			feature:  geojson.NewFeature(orb.Point{rand.Float64(), rand.Float64()}),
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "Replace Non-Existing ID",
			feature:  newFeatureWithID(orb.Point{rand.Float64(), rand.Float64()}, "non-existing-id"),
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rr := httptest.NewRecorder()

			// prepare db
			existingFeature := newFeatureWithID(orb.Point{rand.Float64(), rand.Float64()}, "existing-id")
			insert(t, existingFeature, mux, rr)

			body, err := tt.feature.MarshalJSON()
			if err != nil {
				t.Fatal(err)
			}

			req, err := http.NewRequest("POST", "/replace", bytes.NewReader(body))
			if err != nil {
				t.Fatal(err)
			}

			mux.ServeHTTP(rr, req)

			if rr.Code == http.StatusTemporaryRedirect {
				req, err := http.NewRequest("POST", rr.Header().Get("location"), bytes.NewReader(body))
				if err != nil {
					t.Fatal(err)
				}
				rr := httptest.NewRecorder()

				mux.ServeHTTP(rr, req)

				if rr.Code != tt.wantCode {
					t.Errorf("handler returned wrong status code: got %v want %v", rr.Code, tt.wantCode)
				}
			} else if rr.Code != http.StatusOK {
				t.Errorf("handler returned wrong status code: got %v want %v", rr.Code, http.StatusOK)
			}
		})
	}
}

func TestDelete(t *testing.T) {
	mux := http.NewServeMux()

	storage := NewStorage(mux, "test", []string{}, true, "test.json")
	router := NewRouter(mux, [][]string{{"test"}}, "../front/dist")

	go storage.Run()
	go router.Run()

	t.Cleanup(router.Stop)
	t.Cleanup(storage.Stop)
	t.Cleanup(func() { _ = os.Remove("test.json") })

	tests := []struct {
		name     string
		feature  *geojson.Feature
		wantCode int
	}{
		{
			name:     "Delete Existing ID",
			feature:  newFeatureWithID(orb.Point{rand.Float64(), rand.Float64()}, "existing-id"),
			wantCode: http.StatusOK,
		},
		{
			name:     "Delete Non-Existing ID",
			feature:  newFeatureWithID(orb.Point{rand.Float64(), rand.Float64()}, "non-existing-id"),
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rr := httptest.NewRecorder()

			// prepare db
			existingFeature := newFeatureWithID(orb.Point{rand.Float64(), rand.Float64()}, "existing-id")
			insert(t, existingFeature, mux, rr)

			body, err := tt.feature.MarshalJSON()
			if err != nil {
				t.Fatal(err)
			}

			req, err := http.NewRequest("DELETE", "/delete", bytes.NewReader(body))
			if err != nil {
				t.Fatal(err)
			}

			mux.ServeHTTP(rr, req)

			if rr.Code == http.StatusTemporaryRedirect {
				req, err := http.NewRequest("DELETE", rr.Header().Get("location"), bytes.NewReader(body))
				if err != nil {
					t.Fatal(err)
				}
				rr := httptest.NewRecorder()

				mux.ServeHTTP(rr, req)

				if rr.Code != tt.wantCode {
					t.Errorf("handler returned wrong status code: got %v want %v", rr.Code, tt.wantCode)
				}
			} else if rr.Code != http.StatusOK {
				t.Errorf("handler returned wrong status code: got %v want %v", rr.Code, http.StatusOK)
			}
		})
	}
}

func newFeatureWithID(geometry orb.Geometry, id string) *geojson.Feature {
	feature := geojson.NewFeature(geometry)
	feature.ID = id
	return feature
}

func insert(t *testing.T, feature *geojson.Feature, mux *http.ServeMux, rr *httptest.ResponseRecorder) {
	body, err := feature.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}

	req, err := http.NewRequest("POST", "/insert", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}

	mux.ServeHTTP(rr, req)

	if rr.Code == http.StatusTemporaryRedirect {
		req, err := http.NewRequest("POST", rr.Header().Get("location"), bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		rr := httptest.NewRecorder()

		mux.ServeHTTP(rr, req)

		if rr.Code != http.StatusOK {
			t.Errorf("handler returned wrong status code: got %v want %v", rr.Code, http.StatusOK)
		}
	} else if rr.Code != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v", rr.Code, http.StatusOK)
	}
}

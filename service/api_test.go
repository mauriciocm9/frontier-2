package main

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHTTPValidationAndAuthentication(t *testing.T) {
	s, tenant := testStore(t)
	a := &api{store: s, token: "test-token", tenant: tenant}
	cases := []struct {
		body, code string
	}{
		{"{", "E100"},
		{"[]", "E102"},
		{"null", "E102"},
		{"true", "E102"},
		{`{"m":3}`, "E102"},
		{`{"m":""}`, "E103"},
		{`{"m":"PING;"}`, "E104"},
		{`{"m":"ALTA;a;b;XX"}`, "E201"},
		{`{"m":"ALTA;a;b;00"}`, "E202"},
		{`{"m":"ALTA;a;b;23"}`, "E302"},
		{`{"m":"PING;64"} trailing`, "E100"},
	}
	for _, test := range cases {
		r := httptest.NewRequest("POST", "/msg", strings.NewReader(test.body))
		r.Header.Set("Authorization", "Bearer test-token")
		w := httptest.NewRecorder()
		a.ServeHTTP(w, r)
		var body map[string]string
		if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil || w.Code != 400 || body["error"] != test.code {
			t.Fatalf("%s: %d %s; want %s", test.body, w.Code, w.Body, test.code)
		}
	}
	for _, path := range []string{"/health", "/msg", "/__reset", "/unknown"} {
		w := httptest.NewRecorder()
		a.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
		if w.Code != 401 {
			t.Fatalf("unauthenticated %s returned %d", path, w.Code)
		}
	}
	w := httptest.NewRecorder()
	a.ServeHTTP(w, httptest.NewRequest("GET", "/ready", nil))
	if w.Code != 200 {
		t.Fatal("ALB readiness check failed")
	}
}

package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"reflect"
	"time"
)

const (
	apiURL = "https://caja-negra-production-c64f.up.railway.app"
	token  = "cn-p-abh_bog_2dyh_43mg-a1HEnyBUOpBJZuDq8v_d6D"
)

type example struct {
	N        int             `json:"n"`
	Request  json.RawMessage `json:"request"`
	Status   int             `json:"status"`
	Response json.RawMessage `json:"response"`
}

var client = &http.Client{Timeout: 5 * time.Second}

func post(path string, body []byte) (int, []byte, string, error) {
	req, err := http.NewRequest("POST", apiURL+path, bytes.NewReader(body))
	if err != nil {
		return 0, nil, "", err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return 0, nil, "", err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b, resp.Header.Get("X-Req"), nil
}

func sameJSON(a, b []byte) bool {
	var x, y interface{}
	if json.Unmarshal(a, &x) != nil || json.Unmarshal(b, &y) != nil {
		return bytes.Equal(a, b)
	}
	return reflect.DeepEqual(x, y)
}

func main() {
	f, err := os.Open("pruebas/ejemplos.jsonl")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	if s, b, _, err := post("/__reset", nil); err != nil {
		panic(err)
	} else {
		fmt.Printf("reset: %d %s\n", s, b)
	}

	sc := bufio.NewScanner(f)
	pass, total := 0, 0
	for sc.Scan() {
		var ex example
		if err := json.Unmarshal(sc.Bytes(), &ex); err != nil {
			panic(err)
		}
		total++
		status, body, xreq, err := post("/msg", ex.Request)
		if err != nil {
			fmt.Printf("#%d ERROR %v\n", ex.N, err)
			continue
		}
		ok := status == ex.Status && sameJSON(body, ex.Response)
		if ok {
			pass++
		}
		mark := "OK  "
		if !ok {
			mark = "DIFF"
		}
		fmt.Printf("#%02d %s %s x-req=%s -> %d %s\n", ex.N, mark, ex.Request, xreq, status, bytes.TrimSpace(body))
		if !ok {
			fmt.Printf("     expected %d %s\n", ex.Status, ex.Response)
		}
		time.Sleep(120 * time.Millisecond)
	}
	fmt.Printf("\n%d/%d match\n", pass, total)
}

package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/GOphersEngineers/architecture-practice-4-template/httptools"
	"github.com/GOphersEngineers/architecture-practice-4-template/signal"
)

type Response struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type Request struct {
	Value string `json:"value"`
}

var port = flag.Int("port", 8080, "server port")

const confResponseDelaySec = "CONF_RESPONSE_DELAY_SEC"
const confHealthFailure = "CONF_HEALTH_FAILURE"
const db_Url = "http://db:8083/db"

func main() {
	h := new(http.ServeMux)
	client := http.DefaultClient

	h.HandleFunc("/health", func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("content-type", "text/plain")
		if failConfig := os.Getenv(confHealthFailure); failConfig == "true" {
			rw.WriteHeader(http.StatusInternalServerError)
			_, _ = rw.Write([]byte("FAILURE"))
		} else {
			rw.WriteHeader(http.StatusOK)
			_, _ = rw.Write([]byte("OK"))
		}
	})

	report := make(Report)

	h.HandleFunc("/api/v1/some-data", func(rw http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		resp, err := client.Get(fmt.Sprintf("%s/%s", db_Url, key))
		if err != nil {
			rw.WriteHeader(http.StatusInternalServerError)
			return
		}

		if !(resp.StatusCode >= 200 && resp.StatusCode < 300) {
			rw.WriteHeader(resp.StatusCode)
			return
		}

		respDelayString := os.Getenv(confResponseDelaySec)
		if delaySec, parseErr := strconv.Atoi(respDelayString); parseErr == nil && delaySec > 0 && delaySec < 300 {
			time.Sleep(time.Duration(delaySec) * time.Second)
		}

		report.Process(r)

		var response Response
		json.NewDecoder(resp.Body).Decode(&response)
		rw.Header().Set("content-type", "application/json")
		rw.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(rw).Encode([]string{
			"1", "2",
		})
	})

	h.Handle("/report", report)

	server := httptools.CreateServer(*port, h)
	server.Start()

	buff := new(bytes.Buffer)
	body := Request{Value: time.Now().Format(time.RFC3339)}
	json.NewEncoder(buff).Encode(body)

	res, _ := client.Post(fmt.Sprintf("%s/gophersengineers", db_Url), "application/json", buff)
	defer res.Body.Close()

	signal.WaitForTerminationSignal()
}

package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/GOphersEngineers/architecture-practice-4-template/httptools"
	"github.com/GOphersEngineers/architecture-practice-4-template/signal"
)

var (
	port       = flag.Int("port", 8090, "load balancer port")
	timeoutSec = flag.Int("timeout-sec", 3, "request timeout time in seconds")
	https      = flag.Bool("https", false, "whether backends support HTTPs")

	traceEnabled = flag.Bool("trace", false, "whether to include tracing information into responses")
)

var (
	timeout     = time.Duration(*timeoutSec) * time.Second
	serversPool = []string{
		"server1:8080",
		"server2:8080",
		"server3:8080",
	}
	poolOfHealthyServers = make([]string, len(serversPool))
	serverTraffic        = make(map[string]int64)
	poolLock             sync.Mutex
	healthChecker        HealthChecker
	requestSender        RequestSender
)

type HealthChecker interface {
	Check(string) bool
}

type DefaultHealthChecker struct{}

func (hc *DefaultHealthChecker) Check(dst string) bool {
	ctx, _ := context.WithTimeout(context.Background(), timeout)
	req, _ := http.NewRequestWithContext(ctx, "GET",
		fmt.Sprintf("%s://%s/health", scheme(), dst), nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	if resp.StatusCode != http.StatusOK {
		return false
	}
	return true
}

type RequestSender interface {
	Send(*http.Request) (*http.Response, error)
}

type DefaultRequestSender struct{}

func (rs *DefaultRequestSender) Send(fwdRequest *http.Request) (*http.Response, error) {
	return http.DefaultClient.Do(fwdRequest)
}

func scheme() string {
	if *https {
		return "https"
	}
	return "http"
}

func forward(dst string, rw http.ResponseWriter, r *http.Request) error {
	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()
	fwdRequest := r.Clone(ctx)
	fwdRequest.RequestURI = ""
	fwdRequest.URL.Host = dst
	fwdRequest.URL.Scheme = scheme()
	fwdRequest.Host = dst

	resp, err := requestSender.Send(fwdRequest)
	if err != nil {
		log.Printf("Failed to get response from %s: %s", dst, err)
		rw.WriteHeader(http.StatusServiceUnavailable)
		return err
	}
	defer resp.Body.Close()

	for k, values := range resp.Header {
		for _, value := range values {
			rw.Header().Add(k, value)
		}
	}

	if *traceEnabled {
		rw.Header().Set("lb-from", dst)
	}

	log.Println("fwd", resp.StatusCode, resp.Request.URL)
	rw.WriteHeader(resp.StatusCode)

	n, err := io.Copy(rw, resp.Body)
	if err != nil {
		log.Printf("Failed to write response: %s", err)
		return err
	}

	poolLock.Lock()
	serverTraffic[dst] += n
	poolLock.Unlock()

	return nil
}

func chooseServer() string {
	poolLock.Lock()
	defer poolLock.Unlock()

	var minTrafficServer string
	var minTraffic int64 = -1

	for _, server := range poolOfHealthyServers {
		if traffic, exists := serverTraffic[server]; exists {
			if minTraffic == -1 || traffic < minTraffic {
				minTraffic = traffic
				minTrafficServer = server
			}
		} else {
			serverTraffic[server] = 0
			if minTraffic == -1 {
				minTraffic = 0
				minTrafficServer = server
			}
		}
	}

	return minTrafficServer
}

func healthCheck(servers []string) {
	healthStatus := make(map[string]bool)
	for _, server := range servers {
		healthStatus[server] = true
	}

	for i, server := range servers {
		go func(server string) {
			for range time.Tick(10 * time.Second) {
				isHealthy := healthChecker.Check(server)
				poolLock.Lock()

				if isHealthy {
					healthStatus[server] = true
					poolOfHealthyServers[i] = server
				} else {
					healthStatus[server] = false
					poolOfHealthyServers[i] = ""
				}

				poolOfHealthyServers = nil

				for _, server := range servers {
					if healthStatus[server] {
						poolOfHealthyServers = append(poolOfHealthyServers, server)
					}
				}

				poolLock.Unlock()
				log.Println(server, isHealthy)
			}
		}(server)
	}
}

func main() {
	flag.Parse()

	healthChecker = &DefaultHealthChecker{}
	requestSender = &DefaultRequestSender{}

	healthCheck(serversPool)

	frontend := httptools.CreateServer(*port, http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		server := chooseServer()
		if server == "" {
			http.Error(rw, "No healthy servers available", http.StatusServiceUnavailable)
			return
		}

		err := forward(server, rw, r)
		if err != nil {
			return
		}
	}))

	log.Println("Starting load balancer...")
	log.Printf("Tracing support enabled: %t", *traceEnabled)
	frontend.Start()
	signal.WaitForTerminationSignal()
}
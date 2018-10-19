package arangodb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/internal"
	"github.com/influxdata/telegraf/plugins/inputs"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"sync"
	"time"
)

// HttpJson struct
type ArangoDB struct {
	Urls         	[]string 			`toml:"urls"`
	ResponseTimeout internal.Duration	`toml:"response_timeout"`
	Username        string 				`toml:"username"`
	Password        string 				`toml:"password"`

	client *http.Client
}

type LoginRequest struct {
	Username 		string 				`json:"username"`
	Password 		string 				`json:"password"`
}

type LoginResponse struct {
	Jwt       		string 				`json:"jwt"`
}

type ArangoSystem struct {
	MajorPageFaults		uint32			`json:"majorPageFaults"`
	MinorPageFaults		uint32			`json:"minorPageFaults"`
	NumberOfThreads		uint32			`json:"numberOfThreads"`
	ResidentSize		float32			`json:"residentSize"`
	SystemTime			float32			`json:"systemTime"`
	UserTime			float32			`json:"userTime"`
	VirtualSize			uint64			`json:"virtualSize"`
}

type ArangoRequestTime struct {
	Count				uint32			`json:"requestTime"`
	Counts				[]uint32		`json:"counts"`
	Sum 				float32			`json:"sum"`
}

type ArangoClient struct {
	RequestTime			ArangoRequestTime	`json:"requestTime"`
}

type ArangoServer struct {
	PhysicalMemory 		uint64			`json:"physicalMemory"`
	Uptime 				float32			`json:"uptime"`
}

type ArangoStats struct  {
	Client 				ArangoClient	`json:"client"`
	Server 				ArangoServer	`json:"server"`
	System 				ArangoSystem	`json:"system"`
}

const loginPostfix = "/_open/auth"
const statsPostfix = "/_admin/statistics"

const sampleConfig = `
  ## An array of urls endpoints to get results from
  urls = ["http://localhost:8529"]

  ## Specify timeout duration for slower connections
  # response_timeout = "3s"

  username = "root"
  password = "root"
`

func (p *ArangoDB) SampleConfig() string {
	return sampleConfig
}

func (p *ArangoDB) Description() string {
	return "Read metrics from an ArangoDB server"
}

func (p *ArangoDB) createHttpClient() (*http.Client, error) {

	client := &http.Client{
		Transport: &http.Transport{
			DisableKeepAlives: true,
		},
		Timeout: p.ResponseTimeout.Duration,
	}

	return client, nil
}

func (p *ArangoDB) gatherURL(u url.URL, acc telegraf.Accumulator) error {

	// first get the bearer token by logging in
	loginBody := &LoginRequest{Username: p.Username, Password: p.Password}
	loginBodyJson, err := json.Marshal(loginBody)
	if err != nil {
		return err
	}

	loginReq, err := http.NewRequest("POST", u.String() + loginPostfix, bytes.NewBuffer(loginBodyJson))
	if err != nil {
		return err
	}

	tokenRequest, err := p.client.Do(loginReq)
	if err != nil {
		return fmt.Errorf("error making HTTP Login request to %s: %s", u, err)
	}
	defer tokenRequest.Body.Close()

	body, err := ioutil.ReadAll(tokenRequest.Body)
	if err != nil {
		return fmt.Errorf("error reading token body: %s", err)
	}

	jwtToken := LoginResponse{}
	err = json.Unmarshal(body, &jwtToken)

	if err != nil {
		return err
	}


	// at this point we've got a bearer token, and can use that to log in. Simple get
	// so ignore the error
	statsReq, _ := http.NewRequest("GET", u.String() + statsPostfix, nil)
	statsReq.Header.Set("Authorization", "Bearer " + string(jwtToken.Jwt))

	statsResponse, _ := p.client.Do(statsReq)

	if err != nil {
		return fmt.Errorf("error making HTTP Stats request to %s: %s", u, err)
	}
	defer statsResponse.Body.Close()

	body, err = ioutil.ReadAll(statsResponse.Body)
	if err != nil {
		return fmt.Errorf("error reading stats body: %s", err)
	}

	// parse the stats into a go object
	stats := ArangoStats{}
	err = json.Unmarshal(body, &stats)
	if err != nil {
		return err
	}

	// create the arguments for the accumulator
	tags := make(map[string]string)
	tags["url"] = u.String()

	systemFields := make(map[string]interface{})
	systemFields["majorPageFaults"] = stats.System.MajorPageFaults
	systemFields["minorPageFaults"] = stats.System.MinorPageFaults
	systemFields["numberOfThreads"] = stats.System.NumberOfThreads
	systemFields["residentSize"] = stats.System.ResidentSize
	systemFields["systemTime"] = stats.System.SystemTime
	systemFields["userTime"] = stats.System.UserTime
	systemFields["virtualSize"] = stats.System.VirtualSize

	serverFields := make(map[string]interface{})
	serverFields["physicalMemory"] = stats.Server.PhysicalMemory
	serverFields["uptime"] = stats.Server.Uptime

	clientFields := make(map[string]interface{})
	clientFields["req_0.01"] = stats.Client.RequestTime.Counts[0]
	clientFields["req_0.05"] = stats.Client.RequestTime.Counts[1]
	clientFields["req_0.1"] = stats.Client.RequestTime.Counts[2]
	clientFields["req_0.2"] = stats.Client.RequestTime.Counts[3]
	clientFields["req_0.5"] = stats.Client.RequestTime.Counts[4]
	clientFields["req_1"] = stats.Client.RequestTime.Counts[5]
	clientFields["count"] = stats.Client.RequestTime.Count
	clientFields["sum"] = stats.Client.RequestTime.Sum

	// add all th fields
	acc.AddFields("arangodb_system", systemFields, tags)
	acc.AddFields("arangodb_server", serverFields, tags)
	acc.AddFields("arangodb_client", clientFields, tags)

	return nil
}

func (p *ArangoDB) Gather(acc telegraf.Accumulator) error {
	client, err := p.createHttpClient()
	if err != nil {
		return err
	}
	p.client = client

	var wg sync.WaitGroup

	// create URLs based on the configuration
	allURLs := make([]url.URL, 0)
	for _, u := range p.Urls {
		URL, err := url.Parse(u)
		if err != nil {
			log.Printf("arangodb: Could not parse %s, skipping. Error: %s", u, err)
			continue
		}

		allURLs = append(allURLs, *URL)
	}

	// call all the urls and wait for them to finish.
	for _, URL := range allURLs {
		wg.Add(1)
		go func(serviceURL url.URL) {
			defer wg.Done()
			acc.AddError(p.gatherURL(serviceURL, acc))
		}(URL)
	}

	wg.Wait()
	return nil
}


func init() {
	inputs.Add("arangoDB", func() telegraf.Input {
		return &ArangoDB{ResponseTimeout: internal.Duration{Duration: time.Second * 3}}
	})
}

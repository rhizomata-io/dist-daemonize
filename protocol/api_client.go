package protocol

import (
	"bytes"
	"net/http"
	"strings"
)

//Client API client
type Client struct {
	daemonURL string
}

// CheckHealth ..
func CheckHealth(daemonURL string) (remoteID string, err error) {
	resp, err := http.Head(daemonURL + V1Path + HealthPath)
	if err == nil && resp.StatusCode == 200 {
		body := []byte{}
		length, err2 := resp.Body.Read(body)
		if err2 != nil {
			err = err2
		} else {
			remoteID = string(body[:length])
		}
	}
	return remoteID, err
}

// NewClient ..
func NewClient(daemonURL string) (client *Client) {
	apiClient := Client{daemonURL: daemonURL}
	return &apiClient
}

// Health check health
func (client *Client) Health() (remoteID string, err error) {
	return CheckHealth(client.daemonURL)
}

// AddJob ..
func (client *Client) AddJob(data []byte) bool {
	buffer := bytes.Buffer{}
	buffer.Write(data)
	resp, err := http.Post(client.daemonURL+V1Path+AddJobPath, "text/json", &buffer)
	return (err == nil && resp.StatusCode == 200)
}

// AddJobWithID ..
func (client *Client) AddJobWithID(jobid string, data []byte) bool {
	buffer := bytes.Buffer{}
	buffer.Write(data)
	path := strings.Replace(AddJobWithIDPath, ":jobid", jobid, -1)
	resp, err := http.Post(client.daemonURL+V1Path+path, "text/json", &buffer)
	return (err == nil && resp.StatusCode == 200)
}

// RemoveJob ..
func (client *Client) RemoveJob(jobid string) bool {
	buffer := bytes.Buffer{}
	buffer.Write([]byte(jobid))
	resp, err := http.Post(client.daemonURL+V1Path+RemoveJobPath, "text/json", &buffer)
	return (err == nil && resp.StatusCode == 200)
}

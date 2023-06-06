package main

import (
	"encoding/json"
	"log"
	"math/rand"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// struct to unmarshall incoming messages
type Duty struct {
	Validator string `json:"validator"`
	Duty      string `json:"duty"`
	Height    uint   `json:"height"`
}

func main() {
	// utilize Gorilla websocket client to connect to API
	u := url.URL{Scheme: "ws", Host: "127.0.0.1:5000", Path: "/ws"}
	log.Printf("connecting to %s", u.String())
	// Dial to ws
	c, resp, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		log.Printf("handshake failed with status %d", resp.StatusCode)
		log.Fatal("dial:", err)
	}
	// close the connection in case of error
	defer c.Close()
	// run main routine
	Worker(c)
}

func Worker(c *websocket.Conn) {
	// interrupt channel needed to send a kill signal coming form the OS
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	// utilize done channel to close the connection in case of irrecoverable error at client
	done := make(chan struct{})

	// create a channel to recieve processed messages from ws
	duties := make(chan Duty)

	// channels of processed messages for validators
	proposerDuties := make(chan Duty)
	aggregtorDuties := make(chan Duty)
	attesterDuties := make(chan Duty)
	commiteeDuties := make(chan Duty)

	// Process incoming ws messages
	go ListenAndProcess(done, c, duties)

	// run execution in parallel
	go Proposer(proposerDuties)
	go Aggregator(aggregtorDuties)
	go Attester(attesterDuties)
	go Committee(commiteeDuties)

	var wg sync.WaitGroup
	for {
		select {
		case duty := <-duties:
			wg.Add(1)
			// process different types of messages
			if duty.Duty == "PROPOSER" {
				go func() {
					defer wg.Done()
					ProposerProcessor(duty, proposerDuties)
				}()
			}
			if duty.Duty == "ATTESTER" {
				go func() {
					defer wg.Done()
					AttesterProcessor(duty, attesterDuties)
				}()
			}
			if duty.Duty == "AGGREGATOR" {
				go func() {
					defer wg.Done()
					AggregatorProcessor(duty, aggregtorDuties)
				}()
			}
			if duty.Duty == "SYNC_COMMITTEE" {
				go func() {
					defer wg.Done()
					CommitteeProcessor(duty, commiteeDuties)
				}()
			}
		case <-done:
			return
		case <-interrupt:
			log.Println("interrupt")
			err := c.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
			if err != nil {
				log.Println("write close:", err)
				return
			}
			return
		}
	}
}

func ListenAndProcess(done chan struct{}, c *websocket.Conn, d chan Duty) {
	defer close(done)
	for {
		// read message from ws
		_, message, err := c.ReadMessage()
		if err != nil {
			log.Println("read:", err)
			return
		}
		var duty Duty
		err = json.Unmarshal(message, &duty)
		if err != nil {
			log.Fatal("Cant unmarshall message:", err)
		}
		log.Printf("Message received: %v", duty)
		// send message to channel for processing
		d <- duty
	}
}

// functions for duties processing
func ProposerProcessor(m Duty, processedDuties chan Duty) {
	n := 1 + rand.Int31n(20-1+1)
	time.Sleep(time.Second * time.Duration(n))
	log.Printf("Duty PROPOSER processed: %v", m)
	processedDuties <- m
}

func AttesterProcessor(m Duty, processedDuties chan Duty) {
	n := 1 + rand.Int31n(20-1+1)
	time.Sleep(time.Second * time.Duration(n))
	log.Printf("Duty ATTESTER processed: %v", m)
	processedDuties <- m
}

func AggregatorProcessor(m Duty, processedDuties chan Duty) {
	n := 1 + rand.Int31n(20-1+1)
	time.Sleep(time.Second * time.Duration(n))
	log.Printf("Duty AGGREGATOR processed: %v", m)
	processedDuties <- m
}

func CommitteeProcessor(m Duty, processedDuties chan Duty) {
	n := 1 + rand.Int31n(20-1+1)
	time.Sleep(time.Second * time.Duration(n))
	log.Printf("Duty SYNC_COMMITTEE processed: %v", m)
	processedDuties <- m
}

// main execution routines
func Proposer(d chan Duty) {
	for {
		select {
		case duty := <-d:
			n := 1 + rand.Int31n(20-1+1)
			time.Sleep(time.Second * time.Duration(n))
			log.Printf("Duty PROPOSER executed: %v", duty)
		}
	}
}

func Attester(d chan Duty) {
	for {
		select {
		case duty := <-d:
			n := 1 + rand.Int31n(20-1+1)
			time.Sleep(time.Second * time.Duration(n))
			log.Printf("Duty ATTESTER executed: %v", duty)
		}
	}
}

func Aggregator(d chan Duty) {
	for {
		select {
		case duty := <-d:
			n := 1 + rand.Int31n(20-1+1)
			time.Sleep(time.Second * time.Duration(n))
			log.Printf("Duty AGGREGATOR executed: %v", duty)
		}
	}
}

func Committee(d chan Duty) {
	for {
		select {
		case duty := <-d:
			n := 1 + rand.Int31n(20-1+1)
			time.Sleep(time.Second * time.Duration(n))
			log.Printf("Duty SYNC_COMMITTEE executed: %v", duty)
		}
	}
}

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

type Duty struct {
	Validator string `json:"validator"`
	Duty      string `json:"duty"`
	Height    uint   `json:"height"`
}

func main() {
	// interrupt channel needed to send a kill signal coming form the OS
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
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

	// utilize done channel to close the connection in case of irrecoverable error at client
	done := make(chan struct{})

	// create a channel to recieve processed messages from ws
	duties := make(chan Duty)

	// Process incoming ws messages
	go listenAndProcess(done, c, duties)

	// sync group to process each message in parrallel
	var wg sync.WaitGroup
	wg.Wait()
	for {
		select {
		case duty := <-duties:
			wg.Add(1)
			// process different types of messages
			if duty.Duty == "PROPOSER" {
				go func() {
					defer wg.Done()
					proposerProcessor(duty)
				}()

			}
			if duty.Duty == "ATTESTER" {
				go func() {
					defer wg.Done()
					attesterProcessor(duty)
				}()
			}
			if duty.Duty == "AGGREGATOR" {
				go func() {
					defer wg.Done()
					aggregatorProcessor(duty)
				}()
			}
			if duty.Duty == "SYNC_COMMITTEE" {
				go func() {
					defer wg.Done()
					committeeProcessor(duty)
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

func listenAndProcess(done chan struct{}, c *websocket.Conn, d chan Duty) {
	defer close(done)
	for {
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
		d <- duty
	}
}

func proposerProcessor(m Duty) {
	n := 1 + rand.Int31n(20-1+1)
	time.Sleep(time.Second * time.Duration(n))
	log.Printf("Duty PROPOSER processed: %v", m)
}

func attesterProcessor(m Duty) {
	n := 1 + rand.Int31n(20-1+1)
	time.Sleep(time.Second * time.Duration(n))
	log.Printf("Duty ATTESTER processed: %v", m)
}

func aggregatorProcessor(m Duty) {
	n := 1 + rand.Int31n(20-1+1)
	time.Sleep(time.Second * time.Duration(n))
	log.Printf("Duty AGGREGATOR processed: %v", m)
}

func committeeProcessor(m Duty) {
	n := 1 + rand.Int31n(20-1+1)
	time.Sleep(time.Second * time.Duration(n))
	log.Printf("Duty SYNC_COMMITTEE processed: %v", m)
}

package main_test

import (
	"encoding/json"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"os/signal"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	dp "github.com/pavelkrolevets/DutyProcessor"
)

var upgrader = websocket.Upgrader{}

func echo(w http.ResponseWriter, r *http.Request) {
    c, err := upgrader.Upgrade(w, r, nil)
    if err != nil {
        return
    }
    defer c.Close()
    for {
        mt, message, err := c.ReadMessage()
        if err != nil {
            break
        }
        err = c.WriteMessage(mt, message)
        if err != nil {
            break
        }
    }
}

func TestWorker(t *testing.T) {
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	
    // Create test server with the echo handler.
    s := httptest.NewServer(http.HandlerFunc(echo))
    defer s.Close()

    // Convert http://127.0.0.1 to ws://127.0.0.
    u := "ws" + strings.TrimPrefix(s.URL, "http")

    // Connect to the server
    ws, _, err := websocket.DefaultDialer.Dial(u, nil)
    if err != nil {
        t.Fatalf("%v", err)
    }
    defer ws.Close()
	var msgs [][]byte
	for i:=0;i<=100;i++ {
		n := 1 + rand.Int31n(10-1+1)
		h := 1 + rand.Int31n(1000-1+1)
		var testDuty dp.Duty
		testDuty.Duty = "PROPOSER"
		testDuty.Height = uint(h)
		testDuty.Validator = string(n)
		m, err := json.Marshal(testDuty)
		if err!=nil {
			t.Fatal(err)
		}
		msgs = append(msgs, m)
	}
	dp.Worker(ws, interrupt)
    // Send message to server, read response and check to see if it's what we expect.
    for _, msg := range(msgs) {
		time.Sleep(time.Second * 1)
        if err := ws.WriteMessage(websocket.TextMessage, msg); err != nil {
            t.Fatalf("%v", err)
        }
    }	
}
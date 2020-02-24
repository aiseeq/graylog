package graylog

import (
	"fmt"
	"github.com/json-iterator/go"
	"log"
	"net"
	"os"
	"time"
)

// Endpoint represents a graylog endpoint
type Endpoint struct {
	Address string
	Port    uint
}

// Graylog represents an established graylog connection
type Graylog struct {
	Client *net.UDPConn
}

// Message represents a GELF formated message
type Message struct {
	Version      string                 `json:"version"`
	Host         string                 `json:"host"`
	ShortMessage string                 `json:"short_message"`
	FullMessage  string                 `json:"full_message,omitempty"`
	Timestamp    int64                  `json:"timestamp,omitempty"`
	Level        uint                   `json:"level,omitempty"`
	Extra        map[string]interface{} `json:"-"`
}

var queue chan Message
var g *Graylog
var hostname string

// NewGraylog instanciates a new graylog connection using the given endpoint
func newGraylog(e Endpoint) (*Graylog, error) {
	ra, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", e.Address, e.Port))
	if err != nil {
		return nil, err
	}
	la, err := net.ResolveUDPAddr("udp", ":0")
	if err != nil {
		return nil, err
	}
	c, err := net.DialUDP("udp", la, ra)
	if err != nil {
		return nil, err
	}

	return &Graylog{Client: c}, nil
}

// Send writes the given message to the given graylog client
func (g *Graylog) deliver(m Message) error {
	data, err := prepareMessage(m)
	if err != nil {
		return err
	}
	// log.Println("Debug: Sending data: " + string(data))
	_, err = (*g.Client).Write(data)

	return err
}

// Close closes the opened connections of the given client
func (g *Graylog) close() error {
	if g.Client != nil {
		if err := (*g.Client).Close(); err != nil {
			return err
		}
	}

	return nil
}

// prepareMessage marshal the given message, add extra fields and append EOL symbols
func prepareMessage(m Message) ([]byte, error) {
	output := map[string]interface{}{
		"version":       m.Version,
		"host":          m.Host,
		"short_message": m.ShortMessage,
		"full_message":  m.FullMessage,
		"timestamp":     m.Timestamp,
		"level":         m.Level,
	}
	// Loop on extra fields and inject them into JSON
	for key, value := range m.Extra {
		output["_"+key] = value
	}

	data, err := jsoniter.Marshal(output)
	if err != nil {
		return []byte{}, err
	}

	// Append the \n\0 sequence to the given message in order to indicate
	// to graylog the end of the message
	return append(data, '\n', byte(0)), nil
}

func graylogWorker() {
	for message := range queue {
		if g == nil {
			log.Println("Warning: skipped graylog message")
			continue
		}

		err := g.deliver(message)
		if err != nil {
			log.Println("Error: " + err.Error())
		}
	}
}

func Send(shortMessage string, fullMessage string, level uint, extra map[string]interface{}) {
	queue <- Message{
		Version:      "1.1",
		Host:         hostname,
		ShortMessage: shortMessage,
		FullMessage:  fullMessage,
		Timestamp:    time.Now().Unix(),
		Level:        level,
		Extra:        extra,
	}
}

func InitGraylog(addr string, port uint) {
	var err error
	g, err = newGraylog(Endpoint{
		Address: addr,
		Port:    port,
	})
	if err != nil {
		log.Println("Critical: " + err.Error())
		return
	}

	hostname, _ = os.Hostname()
	queue = make(chan Message, 100)
	go graylogWorker()
}

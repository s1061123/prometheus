package serviceassurance

import (
	"fmt"
	"os"
	"sync"
	"context"
	"strings"

	"github.com/go-kit/kit/log"
        "github.com/prometheus/prometheus/storage"

	"qpid.apache.org/amqp"
	"qpid.apache.org/electron"
)


type Appendable interface {
	Appender() (storage.Appender, error)
}

type ServiceAssuranceManger struct {
	amqpUrl string
	logger log.Logger
	append Appendable
	// add config if required
	mtx sync.RWMutex
	graceConShut chan struct{}
	ctx context.Context
	//ampq specific
	messages chan amqp.Message
	connection electron.Connection
	wgConnection sync.WaitGroup
	prefetch int
}

func NewServiceAssuranceManager (logger log.Logger, amqpurl string, app Appendable) *ServiceAssuranceManger {
	return &ServiceAssuranceManger{
		amqpUrl: amqpurl,
		prefetch: 10,
		append: app,
		logger: logger,
		graceConShut: make(chan struct{}),
		messages: make(chan amqp.Message),
		ctx:	context.Background(),
	}
}

func (m *ServiceAssuranceManger) ListenAMQP () error {
	container := electron.NewContainer(fmt.Sprintf("receive[%v", os.Getpid()))

	m.wgConnection.Add(1)
	go func (urlStr string) {
		defer m.wgConnection.Done()

		url, err := amqp.ParseURL(urlStr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%v", err)
		}

		m.connection, err = container.Dial("tcp", url.Host)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%v", err)
		}
		addr := strings.TrimPrefix(url.Path, "/")
		opts := []electron.LinkOption{electron.Source(addr)}
		if m.prefetch > 0 {
			opts = append(opts, electron.Capacity(m.prefetch), electron.Prefetch(true))
		}
		r, err := m.connection.Receiver(opts...)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%v", err)
		}
		for {
			select {
			case <-r.Done():
				fmt.Printf("Exit1!\n")
			default:
			}
			if rm, err := r.Receive(); err == nil {
				rm.Accept()
				m.messages <- rm.Message
			} else if err == electron.Closed {
				return
			} else {
				fmt.Fprintf(os.Stderr, "receive error %v: %v", urlStr, err)
			}
		}
	} (m.amqpUrl)


	var wait sync.WaitGroup
	for {
		select {
		case mes := <-m.messages:
			if sbody, ok := mes.Body().(amqp.Binary); ok {
				sbodystr := sbody.String()
				wait.Add(1)
				go func (mesg string) {
					defer wait.Done()
					//fmt.Printf("body: %v\n", mesg)
					app, err := m.append.Appender()
					if err != nil {
						fmt.Fprintf(os.Stderr, "failed to get Appender: %v", err)
						return
					}

					collectd, err := ParseCollectdJson(mesg)
					if err != nil {
						fmt.Fprintf(os.Stderr, "failed to parse json: %v", err)
						return
					}

					for _, elem := range *collectd {
						lbls := elem.Labels()
						tval := elem.GetTime()
						for i := 0; i < len(elem.Values); i++ {
						//	fmt.Printf("lablel: %v, t: %v, vals: %v\n", lbls[i], tval, elem.Values[i])
							fmt.Printf(".")
							app.Add(lbls[i], tval, elem.Values[i])
						}
						if err := app.Commit(); err != nil {
							fmt.Fprintf(os.Stderr, "failed to commit tsdb: %v", err)
							return
						}
					}
				}(sbodystr)
			} else {
				fmt.Fprintf(os.Stderr, "Format error: n") //XXX: need logging
			}
		case <-m.ctx.Done():
			m.connection.Close(nil)
			wait.Wait()
			m.wgConnection.Wait()
			return m.ctx.Err()
		}

	}

	return nil // never reach!
}

func (m *ServiceAssuranceManger) Run(ctx context.Context) error {
	m.ctx = ctx

	return m.ListenAMQP()
}

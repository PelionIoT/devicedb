package raft_test

import (
    . "devicedb/raft"
    "github.com/coreos/etcd/raft/raftpb"
    "github.com/gorilla/mux"
    "net"
    "net/http"
    "strconv"
    "time"
    "context"
    "errors"

    . "github.com/onsi/ginkgo"
    . "github.com/onsi/gomega"
)

var (
    PortIndex = 1
    PortBase = 9000
    SenderNodeID = uint64(1)
    ReceiverNodeID = uint64(2)
)

type TestHTTPServer struct {
    port int
    r *mux.Router
    httpServer *http.Server
    listener net.Listener
    done chan int
}

func NewTestHTTPServer(port int) *TestHTTPServer {
    httpServer := &TestHTTPServer{
        port: port,
        done: make(chan int),
        r: mux.NewRouter(),
    }

    return httpServer
}

func (s *TestHTTPServer) Start() error {
    s.httpServer = &http.Server{
        Handler: s.r,
        WriteTimeout: 15 * time.Second,
        ReadTimeout: 15 * time.Second,
    }
    
    var listener net.Listener
    var err error

    listener, err = net.Listen("tcp", "localhost:" + strconv.Itoa(s.port))
    
    if err != nil {
        return err
    }
    
    s.listener = listener

    go func() {
        s.httpServer.Serve(s.listener)
        s.done <- 1
    }()

    return nil
}

func (s *TestHTTPServer) Stop() {
    s.listener.Close()
    <-s.done
}

func (s *TestHTTPServer) Router() *mux.Router {
    return s.r
}

var _ = Describe("Transport", func() {
    var ReceiverPort int
    var SenderPort int
    var receiverServer *TestHTTPServer
    var senderServer *TestHTTPServer
    var sender *TransportHub
    var receiver *TransportHub

    BeforeEach(func() {
        PortIndex += 1
        ReceiverPort = PortBase + 2*PortIndex
        SenderPort = PortBase + 2*PortIndex + 1

        receiverServer = NewTestHTTPServer(ReceiverPort)
        senderServer = NewTestHTTPServer(SenderPort)
        sender = NewTransportHub(SenderNodeID)
        receiver = NewTransportHub(ReceiverNodeID)

        receiver.OnReceive(func(ctx context.Context, msg raftpb.Message) error {
            return nil
        })
        
        sender.OnReceive(func(ctx context.Context, msg raftpb.Message) error {
            return nil
        })

        receiver.Attach(receiverServer.Router())
        sender.Attach(senderServer.Router())

        senderServer.Start()
        receiverServer.Start()
        <-time.After(time.Millisecond * 200)
    })

    AfterEach(func() {
        receiverServer.Stop()
        senderServer.Stop()
    })

    Describe("Sending a message", func() {
        Context("The recipient is not known by the sender", func() {
            It("Send should result in an error", func() {
                Expect(sender.Send(context.TODO(), raftpb.Message{
                    From: SenderNodeID,
                    To: ReceiverNodeID,
                }, false)).Should(Equal(EReceiverUnknown))
            })
        })

        Context("The recipient is known by the sender", func() {
            BeforeEach(func() {
                sender.AddPeer(PeerAddress{
                    NodeID: ReceiverNodeID,
                    Host: "localhost",
                    Port: ReceiverPort,
                })
            })

            Context("The sender is known by the recipient", func() {
                BeforeEach(func() {
                    receiver.AddPeer(PeerAddress{
                        NodeID: SenderNodeID,
                        Host: "localhost",
                        Port: SenderPort,
                    })
                })

                Specify("Send should return nil once the receiver has processed the message and responded with an acknowledgment", func() {
                    receiver.OnReceive(func(ctx context.Context, msg raftpb.Message) error {
                        return nil
                    })

                    Expect(sender.Send(context.TODO(), raftpb.Message{
                        From: SenderNodeID,
                        To: ReceiverNodeID,
                    }, false)).Should(BeNil())
                })

                Specify("Send should return an error if the receiver encountered an error processing the message and responded with an error code", func() {
                    receiver.OnReceive(func(ctx context.Context, msg raftpb.Message) error {
                        return errors.New("Something bad happened")
                    })

                    Expect(sender.Send(context.TODO(), raftpb.Message{
                        From: SenderNodeID,
                        To: ReceiverNodeID,
                    }, false)).Should(Not(BeNil()))
                })

                Specify("Send shoult time out and return an ETimeout error if the receiver does not respond within the timeout window", func() {
                    receiver.OnReceive(func(ctx context.Context, msg raftpb.Message) error {
                        <-time.After(time.Second * RequestTimeoutSeconds + time.Second * 2)

                        return nil
                    })

                    Expect(sender.Send(context.TODO(), raftpb.Message{
                        From: SenderNodeID,
                        To: ReceiverNodeID,
                    }, false)).Should(Equal(ETimeout))
                })
            })
        })
    })
})

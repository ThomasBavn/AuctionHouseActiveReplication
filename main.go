package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net"
	"os"
	"strconv"
	"time"

	node "github.com/Grumlebob/PeerToPeer/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type peer struct {
	node.UnimplementedNodeServer
	id             int32
	lamportTime    int32
	responseNeeded int32
	state          string
	clients        map[int32]node.NodeClient
	ctx            context.Context
}

const (
	RELEASED = "Released"
	WANTED   = "Wanted"
	HELD     = "Held"
)

func main() {
	arg1, _ := strconv.ParseInt(os.Args[1], 10, 32)
	ownPort := int32(arg1) + 5000

	log.SetFlags(log.Ltime)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := &peer{
		id:             ownPort,
		lamportTime:    0,
		responseNeeded: 0,
		clients:        make(map[int32]node.NodeClient),
		ctx:            ctx,
		state:          RELEASED,
	}

	// Create listener tcp on port ownPort
	list, err := net.Listen("tcp", fmt.Sprintf("localhost:%v", ownPort))
	if err != nil {
		log.Fatalf("Failed to listen on port: %v", err)
	}
	grpcServer := grpc.NewServer()
	node.RegisterNodeServer(grpcServer, p)

	go func() {
		if err := grpcServer.Serve(list); err != nil {
			log.Fatalf("failed to server %v", err)
		}
	}()

	for i := 0; i < 3; i++ {
		port := int32(5000) + int32(i)
		if port == ownPort {
			continue
		}

		var conn *grpc.ClientConn
		fmt.Printf("Trying to dial: %v\n", port)
		conn, err := grpc.Dial(fmt.Sprintf("localhost:%v", port), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
		if err != nil {
			log.Fatalf("Could not connect: %s", err)
		}
		defer conn.Close()
		c := node.NewNodeClient(conn)
		p.clients[port] = c
	}

	//We need N-1 responses to enter the critical section
	p.responseNeeded = int32(len(p.clients))

	//They all try to access the critical section after a random delay of 4 sec
	go func() {
		randomPause(4)
		p.RequestEnterToCriticalSection(p.ctx, &node.Request{Id: p.id, State: p.state, LamportTime: p.lamportTime})
	}()

	scanner := bufio.NewScanner(os.Stdin)
	//Enter to make client try to go into critical section
	for scanner.Scan() {
		go p.RequestEnterToCriticalSection(p.ctx, &node.Request{Id: p.id, State: p.state, LamportTime: p.lamportTime})
	}
}

func (p *peer) HandlePeerRequest(ctx context.Context, req *node.Request) (*node.Reply, error) {
	//p er den client der svarer på requesten.
	//req kommer fra anden peer.
	//Reply er det svar peer får.
	if p.state == WANTED {
		if req.State == RELEASED {
			p.responseNeeded--
		}
		if req.State == WANTED {
			if req.LamportTime > p.lamportTime {
				p.responseNeeded--
			} else if req.LamportTime == p.lamportTime && req.Id < p.id {
				p.responseNeeded--
			}
		}
	}
	p.lamportTime = int32(math.Max(float64(p.lamportTime), float64(req.LamportTime))) + 1
	reply := &node.Reply{Id: p.id, State: p.state, LamportTime: p.lamportTime}
	return reply, nil
}

func (p *peer) RequestEnterToCriticalSection(ctx context.Context, req *node.Request) (*node.Reply, error) {
	//P Requests to enter critical section, and sends a request to all other peers.
	//WANTS TO ENTER
	if p.state == WANTED || p.state == HELD {
		return &node.Reply{Id: p.id, State: p.state, LamportTime: p.lamportTime}, nil
	}
	p.lamportTime++
	p.state = WANTED
	p.responseNeeded = int32(len(p.clients))
	log.Printf("%v requests entering CS. Timestamp: %v \n", p.id, p.lamportTime)
	p.sendMessageToAllPeers()
	for p.responseNeeded > 0 {
		//It decrements in HandlePeerRequest method.
	}
	if p.responseNeeded == 0 {
		p.TheSimulatedCriticalSection()
	}
	//Out of the critical section
	reply := &node.Reply{Id: p.id, State: p.state, LamportTime: p.lamportTime}
	return reply, nil
}

func (p *peer) TheSimulatedCriticalSection() {
	log.Printf("%v in CS. Timestamp: %v \n", p.id, p.lamportTime)
	p.lamportTime++
	p.state = HELD
	time.Sleep(3 * time.Second)
	//EXITING CRITICAL SECTION
	p.lamportTime++
	p.responseNeeded = int32(len(p.clients))
	p.state = RELEASED
	log.Printf("%v out of CS. Timestamp: %v \n", p.id, p.lamportTime)
	p.sendMessageToAllPeers()
}

func (p *peer) sendMessageToAllPeers() {
	p.lamportTime++
	request := &node.Request{Id: p.id, State: p.state, LamportTime: p.lamportTime}
	for _, client := range p.clients {
		reply, err := client.HandlePeerRequest(p.ctx, request)
		if err != nil {
			log.Println("something went wrong")
		}
		if reply.State == RELEASED {
			p.responseNeeded--
		}
	}
}

func randomPause(max int) {
	rand.Seed(time.Now().UnixNano())
	time.Sleep(time.Millisecond * time.Duration(rand.Intn(max*1000)))
}

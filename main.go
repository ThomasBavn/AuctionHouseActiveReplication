package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	node "github.com/Grumlebob/AuctionSystemReplication/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

type peer struct {
	node.UnimplementedNodeServer
	id                            int32
	agreementsNeededFromBackups   int32
	auctionState                  string
	replicationRole               string
	idOfPrimaryReplicationManager int32
	highestBidOnCurrentAuction    int32
	clients                       map[int32]node.NodeClient
	requestsHandled               map[int32]string
	ctx                           context.Context
}

//Noter fra læren: can Front End be together with client (yes) - And it can be placed in server side, (important in active replication)

//Primary-backup replication implementation.

//• 1. Request: The front end issues the request, containing a unique identifier, to the primary.
//• 2. Coordination: The primary takes each request atomically, in the order in which
//it receives it. It checks the unique identifier, in case it has already executed the
//request, and if so it simply resends the response.
//• 3. Execution: The primary executes the request and stores the response.
//• 4. Agreement: If the request is an update, then the primary sends the updated
//state, the response and the unique identifier to all the backups. The backups send
//an acknowledgement.
//• 5. Response: The primary responds to the front end, which hands the response
//back to the client.

//If primary fails: Leader election! (Ring, bully, raft).
//But we have assumption only 1 will crash, so we don’t need to run bully, we can just communicate with eachother. We can ping each other every ms (like raft heartbeat), if primary is dead, then go to backup and communicate to all. Due to assumptions made in this intro course, we only assume 1 crash, and we can just go be alphabetical order, or id.

//Tldr lav backupleader til maxleader -1

//Unique BidId =  85000  Local counter, 1...2..3 + deres port. Tilføj til map RequestsHandled
//If requesthandled = True, then just send ack. If not, then send ack and update Map.

const (
	// Node role for primary-back replication
	PRIMARY = "PRIMARY"
	BACKUP  = "BACKUP"
)

const (
	// Auction state
	OPEN   = "Open"
	CLOSED = "Closed"
)

var uniqueIdentifier = int32(0)

func main() {
	arg1, _ := strconv.ParseInt(os.Args[1], 10, 32)
	ownPort := int32(arg1) + 5000

	log.SetFlags(log.Ltime)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := &peer{
		id:                          ownPort,
		agreementsNeededFromBackups: 0,
		clients:                     make(map[int32]node.NodeClient),
		ctx:                         ctx,
		auctionState:                CLOSED,
		replicationRole:             BACKUP,
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

	shouldBeRole := BACKUP
	highestClientSeen := int32(0)
	for i, _ := range p.clients {
		if i < ownPort && ownPort > highestClientSeen {
			shouldBeRole = PRIMARY
			highestClientSeen = ownPort
		} else if i > ownPort {
			highestClientSeen = i
			shouldBeRole = BACKUP
		}
	}

	p.replicationRole = shouldBeRole
	p.idOfPrimaryReplicationManager = highestClientSeen
	fmt.Printf("Replication role: %v with id of primary: %v \n", p.replicationRole, p.idOfPrimaryReplicationManager)

	//We need ack responses from the backups.
	p.agreementsNeededFromBackups = int32(len(p.clients))

	//They all try to access the critical section after a random delay of 4 sec
	go func() {
		randomPause(4)
	}()

	scanner := bufio.NewScanner(os.Stdin)
	//Enter to make client try to go into critical section
	for scanner.Scan() {
		text := scanner.Text()
		FindAllNumbers := regexp.MustCompile(`\d+`).FindAllString(text, 1)
		if len(FindAllNumbers) > 0 {
			numeric, _ := strconv.ParseInt(FindAllNumbers[0], 10, 32)
			if strings.Contains(text, "Bid") {
				go p.Bid(p.ctx, &node.Bid{ClientId: p.id, UniqueBidId: p.getUniqueIdentifier(), Amount: int32(numeric)})
			}
		}
	}
}

func (p *peer) getUniqueIdentifier() (uniqueId int32) {
	uniqueIdentifier++
	return uniqueIdentifier
}

func (p *peer) HandleAgreementFromLeader(ctx context.Context, empty *emptypb.Empty) (*node.Acknowledgement, error) {
	reply := &node.Acknowledgement{Ack: "OK"}
	return reply, nil
}

func (p *peer) getAgreementFromAllPeers() (agreementReached bool) {
	p.agreementsNeededFromBackups = int32(len(p.clients))
	for _, client := range p.clients {
		response, err := client.HandleAgreementFromLeader(p.ctx, &emptypb.Empty{})
		if err != nil {
			log.Println("something went wrong")
			return false
		}
		if response.Ack == "OK" {
			p.agreementsNeededFromBackups--
		}

	}
	/*
		for p.agreementsNeededFromBackups > 0 {
			//It decrements in HandlePeerRequest method.
		}
		if p.agreementsNeededFromBackups == 0 {
			p.TheSimulatedCriticalSection()
		}
	*/
	return true
}

func (p *peer) sendMessageToPeerWithId(peerId int32) (rep *node.Acknowledgement, theError error) {

	_, err := p.clients[peerId].HandleAgreementFromLeader(p.ctx, &emptypb.Empty{})
	if err != nil {
		log.Println("something went wrong")
	}
	reply := &node.Acknowledgement{Ack: "OK"}
	return reply, err
}

func (p *peer) Bid(ctx context.Context, bid *node.Bid) (*node.Acknowledgement, error) {

	if (p.replicationRole) == PRIMARY {
		//If primary, then send message to backups
		p.getAgreementFromAllPeers()
	} else {
		//If backup, then send message to primary
		p.sendMessageToPeerWithId(p.idOfPrimaryReplicationManager)
	}

	for p.agreementsNeededFromBackups > 0 {
		//It decrements in HandlePeerRequest method.
	}
	if p.agreementsNeededFromBackups == 0 {

	}

	//Out of the critical section
	reply := &node.Acknowledgement{}
	return reply, nil
}

func (p *peer) Result(ctx context.Context, empty *emptypb.Empty) (*node.Outcome, error) {

	fmt.Println(empty)
	replyFromPrimary, err := p.sendMessageToPeerWithId(p.idOfPrimaryReplicationManager)

	fmt.Println(replyFromPrimary)
	if err != nil {
		log.Println("something went wrong")
		delete(p.clients, p.idOfPrimaryReplicationManager)

	}

	if p.auctionState == OPEN {

	} else if p.auctionState == CLOSED {

	}

	reply := &node.Outcome{}
	return reply, nil
}

func (p *peer) openAuction() {
	p.auctionState = OPEN
	p.agreementsNeededFromBackups = int32(len(p.clients))
	p.highestBidOnCurrentAuction = 0
}

func (p *peer) closeAuction() {
	p.auctionState = CLOSED
	p.agreementsNeededFromBackups = int32(len(p.clients))
}

func randomPause(max int) {
	rand.Seed(time.Now().UnixNano())
	time.Sleep(time.Millisecond * time.Duration(rand.Intn(max*1000)))
}

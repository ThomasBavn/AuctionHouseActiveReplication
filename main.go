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

//If primary fails: Leader election! But we have assumption only 1 will crash, if primary is dead, then go to backup with id-1.

//Unique BidId =  85000  Local counter, 1...2..3 + deres port. Tilføj til map RequestsHandled
//If requesthandled = not nil, resend response.

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
		requestsHandled:             make(map[int32]string),
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

	//Open and close auctions after set timers
	if p.replicationRole == PRIMARY {
		go func() {
			items := []string{"Laptop", "Phone", "Tablet", "TV", "Headphones", "Watch"}
			for _, item := range items {
				p.openAuction(item)
				time.Sleep(30 * time.Second)
				p.closeAuction()
				time.Sleep(15 * time.Second)
			}
		}()
	}
	//Send heartbeats to primary
	if p.replicationRole == BACKUP {
		go func() {
			for {
				p.sendHeartbeatPingToPeerId(p.idOfPrimaryReplicationManager)
				time.Sleep(1 * time.Second)
			}
		}()
	}

	scanner := bufio.NewScanner(os.Stdin)
	//Enter to make client try to go into critical section
	for scanner.Scan() {
		text := scanner.Text()
		FindAllNumbers := regexp.MustCompile(`\d+`).FindAllString(text, 1)
		if len(FindAllNumbers) > 0 {
			numeric, _ := strconv.ParseInt(FindAllNumbers[0], 10, 32)
			if strings.Contains(text, "Bid") {
				if (p.replicationRole == PRIMARY) && (p.auctionState == OPEN) {
					go p.Bid(p.ctx, &node.Bid{ClientId: p.id, UniqueBidId: p.getUniqueIdentifier(), Amount: int32(numeric)})
				} else if (p.replicationRole == BACKUP) && (p.auctionState == OPEN) {
					go p.clients[p.idOfPrimaryReplicationManager].Bid(p.ctx, &node.Bid{ClientId: p.id, UniqueBidId: p.getUniqueIdentifier(), Amount: int32(numeric)})
				}

			}
		}
	}
}

func (p *peer) getUniqueIdentifier() (uniqueId int32) {
	uniqueIdentifier++
	return uniqueIdentifier + p.id
}

func (p *peer) LookAtMeLookAtMeIAmTheCaptainNow() {
	//	               .                       .     .     .        ..*,...*/
	//                                            ...       .   .....,*,...*/**..
	//                                     ,/(@@@@@@@@%**...   .  ...,*,...*/*,..
	// ............................... .//@@@@@@@@@@@@@@@&#,,.......,*....*/*,....
	//                                #&@@@@@@@@@@@@@@@@@@@@@@&,      *    ,/*,
	//                               /&@@@@@@@@@@@@@@@@@@@@@@@@%,     *     /*.
	//                             .(&&&&&&&&&&&&&&&&&&&@@@@@&&&%,    *     *,.
	//                             ,&&&&&&&&&&&&&&&&&&&&&&&&&&&&&,    *     *,.
	//                             *%&&&&&&&&&&&&&&&&&&%%&&&&&&&#     ,    .*,
	//                             ,%&&&&&&&&&%&&&&&&%%%%%&&&&&&*     ,    .*,
	//                             .%&&&%%%%%%%&%&&&&&&&&&&&&&%/,     .    .*,
	//                              %&&&&&&&&&&&&&&%%%%&&&&&&&&%&     .    .*,
	//                             .%%%%&&&%%%&&&%(&&&&%%%%%&&&&*     .    .*,
	//                             ,%%&&&&%##%%&&&&%%%%%%%%&&&%       .    .*,
	//                              /%&&&&##%%&%%&%%%%%(#%%#/         .    .*,
	//                      .(       /%&&%%&&%&&&&&&&&&%(##           .    .*,
	//                      #(     (%/%&&&&&&%%%&&&&&&&%#%*           ..   .*,
	//                     #%    *&%   &&&&&&&&&&&&&&&&&&&##/         ..   .*,
	//           * @ @  %  //(#  #. @ ./ #%&&  * @*  /@ *  #  #@   #/ @  & %*,.
	//           * & % # . /. @ . . , ./ ,#@@  @@@  . @  .&&  @@ & (* @    %**.
	//           * & % % @ /&&@ . . @ ./  .@@.   @ ,* @ *&&&  @, @  * @ ,* @%%%%#(.
	//                   &&&&&&&&&&&&&@@@@@@@@@@@@@@@@&&&&&&&@@@@&&&&&&&&%%%%%%%%%##(
	//                  &&&&&&&&&&@&&@ .( @*  .@ (  &  @@@@@@@@&&&&&&&&&&&&%%%%%%%%%%%
	//                ,&&@@@@@&&&&&&&@  . @  , @ .  / .@@@@@@@@&&&&&&&&&&&%%%%%%%%%%%%
	//               /&&&@@@@@@@&&&&&@ /  @  , @.  #  @@@@@@@@@@&&&&&&&&&%#%%%%%%%&&&%
	//              *&&&&@@@@@@@&&&&&&@@@@@@@@@@@@@@@@@@@/*////*/#(%/(#/*#**%(*/(*
	//
	//
	//If primary fails, just promote a backup to primary. We assume only max 1 crash, therefore just hardcoded to be id 5001.
	if (p.id) == 5001 {
		p.replicationRole = PRIMARY
		p.idOfPrimaryReplicationManager = p.id
	}
}

func (p *peer) openAuction(item string) {
	p.auctionState = OPEN
	p.agreementsNeededFromBackups = int32(len(p.clients))
	p.highestBidOnCurrentAuction = 0
	//for _, client := range p.clients {
	//	p.sendMessageToPeerWithId()
	//}
}

func (p *peer) closeAuction() {
	p.auctionState = CLOSED
	p.agreementsNeededFromBackups = int32(len(p.clients))
}

func randomPause(max int) {
	rand.Seed(time.Now().UnixNano())
	time.Sleep(time.Millisecond * time.Duration(rand.Intn(max*1000)))
}

func (p *peer) getAgreementFromAllPeers() (agreementReached bool) {
	p.agreementsNeededFromBackups = int32(len(p.clients))
	for id, client := range p.clients {
		response, err := client.HandleAgreementFromLeader(p.ctx, &emptypb.Empty{})
		if err != nil {
			log.Printf("Client node %v is dead", id)
			//Remove dead node from list of clients
			delete(p.clients, id)
			//If leader crashed, elect new leader
			if id == p.idOfPrimaryReplicationManager {
				p.LookAtMeLookAtMeIAmTheCaptainNow()
			}
			p.agreementsNeededFromBackups--
		}
		if response.Ack == "OK" {
			p.agreementsNeededFromBackups--
		}

	}
	if p.agreementsNeededFromBackups == 0 {
		return true
	}
	return false
}

func (p *peer) sendHeartbeatPingToPeerId(peerId int32) (rep *node.Acknowledgement, theError error) {
	_, err := p.clients[peerId].HandleAgreementFromLeader(p.ctx, &emptypb.Empty{})
	if err != nil {
		log.Printf("Client node %v is dead", peerId)
		//Remove dead node from list of clients
		delete(p.clients, peerId)
		//If leader crashed, elect new leader
		if peerId == p.idOfPrimaryReplicationManager {
			p.LookAtMeLookAtMeIAmTheCaptainNow()
		}
	}
	reply := &node.Acknowledgement{Ack: "OK"}
	return reply, err
}

func (p *peer) HandleAgreementFromLeader(ctx context.Context, empty *emptypb.Empty) (*node.Acknowledgement, error) {
	reply := &node.Acknowledgement{Ack: "OK"}
	return reply, nil
}

func (p *peer) BroadcastMessage(ctx context.Context, bid *node.Bid) (*node.Acknowledgement, error) {
	agreement := false
	if (p.replicationRole) == PRIMARY {
		//If primary, then send message to backups
		agreement = p.getAgreementFromAllPeers()
	} else {
		//If backup, then send message to primary
		p.clients[p.idOfPrimaryReplicationManager].Bid(ctx, bid)
		return (&node.Acknowledgement{Ack: "Fail, couldn't reach agreement in phase 4"}), nil
	}

	if !agreement {
		return (&node.Acknowledgement{Ack: "Fail, couldn't reach agreement in phase 4"}), nil
	}

	if p.auctionState == CLOSED {
		return (&node.Acknowledgement{Ack: "Fail, auction is closed"}), nil
	}

	//If we reach this point, then we have reached agreement and can check on bid.
	if p.highestBidOnCurrentAuction < bid.Amount {
		p.highestBidOnCurrentAuction = bid.Amount
		return (&node.Acknowledgement{Ack: "OK"}), nil
	} else {
		return (&node.Acknowledgement{Ack: "Fail, your bid was too low"}), nil
	}

	reply := &node.Acknowledgement{}
	return reply, nil
}

func (p *peer) Bid(ctx context.Context, bid *node.Bid) (*node.Acknowledgement, error) {
	agreement := false
	if (p.replicationRole) == PRIMARY {
		//If primary, then send message to backups
		agreement = p.getAgreementFromAllPeers()
	} else {
		//If backup, then send message to primary
		p.clients[p.idOfPrimaryReplicationManager].Bid(ctx, bid)
		return (&node.Acknowledgement{Ack: "Fail, couldn't reach agreement in phase 4"}), nil
	}

	if !agreement {
		return (&node.Acknowledgement{Ack: "Fail, couldn't reach agreement in phase 4"}), nil
	}

	if p.auctionState == CLOSED {
		return (&node.Acknowledgement{Ack: "Fail, auction is closed"}), nil
	}

	//If we reach this point, then we have reached agreement and can check on bid.
	if p.highestBidOnCurrentAuction < bid.Amount {
		p.highestBidOnCurrentAuction = bid.Amount
		return (&node.Acknowledgement{Ack: "OK"}), nil
	} else {
		return (&node.Acknowledgement{Ack: "Fail, your bid was too low"}), nil
	}

	reply := &node.Acknowledgement{}
	return reply, nil
}

func (p *peer) Result(ctx context.Context, empty *emptypb.Empty) (*node.Outcome, error) {

	fmt.Println(empty)
	replyFromPrimary, err := p.sendHeartbeatPingToPeerId(p.idOfPrimaryReplicationManager)

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

package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
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
//state, the response and the unique identifier to all the backups. The backups send an acknowledgement.
//• 5. Response: The primary responds to the front end, which hands the response back to the client.

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
	//3 Peers connected on port 5000, 5001, 5002
	for i := 0; i < 3; i++ {
		port := int32(5000) + int32(i)
		if port == ownPort {
			continue
		}
		var conn *grpc.ClientConn
		//fmt.Printf("Trying to dial: %v\n", port)
		conn, err := grpc.Dial(fmt.Sprintf("localhost:%v", port), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
		if err != nil {
			log.Fatalf("Could not connect: %s", err)
		}
		defer conn.Close()
		c := node.NewNodeClient(conn)
		p.clients[port] = c
	}

	//Assign primary / backup role
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
	log.Printf("Replication role: %v with id of primary as: %v \n", p.replicationRole, p.idOfPrimaryReplicationManager)

	//Open and close auctions after set timers
	if p.replicationRole == PRIMARY {
		go func() {
			items := []string{"Laptop", "Phone", "Tablet", "TV", "Headphones", "Watch"}
			for _, item := range items {
				p.openAuction(item)
				time.Sleep(30 * time.Second)
				p.closeAuction(item)
				time.Sleep(15 * time.Second)
			}
		}()
	}

	//Send heartbeats to primary
	if p.replicationRole == BACKUP {
		go func() {
			for {
				p.sendHeartbeatPingToPeerId(p.idOfPrimaryReplicationManager)
				time.Sleep(300 * time.Millisecond)
				//Backup promoted to Primary
				if p.replicationRole == PRIMARY {
					items := []string{"Table", "Furniture", "YoYo", "Painting", "Diamonds", "Tobacco"}
					for _, item := range items {
						p.openAuction(item)
						time.Sleep(30 * time.Second)
						p.closeAuction(item)
						time.Sleep(10 * time.Second)
					}
				}
			}
		}()
	}

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		text := strings.ToLower(scanner.Text())
		//RESULT COMMAND
		if strings.Contains(text, "result") {
			if p.replicationRole == PRIMARY {
				outcome, _ := p.Result(p.ctx, &emptypb.Empty{})
				log.Printf("%s \nThe highest bid: %v", outcome.AuctionStatus, outcome.HighestBid)
			} else if p.replicationRole == BACKUP {
				p.sendHeartbeatPingToPeerId(p.idOfPrimaryReplicationManager)
				_, foundPrimary := p.clients[p.idOfPrimaryReplicationManager]
				if foundPrimary {
					outcome, _ := p.clients[p.idOfPrimaryReplicationManager].Result(p.ctx, &emptypb.Empty{})
					log.Printf("%s \nThe highest bid: %v", outcome.AuctionStatus, outcome.HighestBid)
				}
			}
		}
		//BID COMMAND
		FindAllNumbers := regexp.MustCompile(`\d+`).FindAllString(text, 1)
		if len(FindAllNumbers) > 0 {
			numeric, _ := strconv.ParseInt(FindAllNumbers[0], 10, 32)
			if strings.Contains(text, "bid") && numeric > 0 {
				uniqueId := p.getUniqueIdentifier()
				//Re-send. Happens in case a primary crashes, and a new one is promoted, that message might have been lost.
				for {
					if p.replicationRole == PRIMARY {
						ack, _ := p.Bid(p.ctx, &node.Bid{ClientId: p.id, UniqueBidId: uniqueId, Amount: int32(numeric)})
						log.Println(ack.Ack)
					} else if p.replicationRole == BACKUP {
						p.sendHeartbeatPingToPeerId(p.idOfPrimaryReplicationManager)
						_, foundPrimary := p.clients[p.idOfPrimaryReplicationManager]
						if foundPrimary {
							ack, _ := p.clients[p.idOfPrimaryReplicationManager].Bid(p.ctx, &node.Bid{ClientId: p.id, UniqueBidId: uniqueId, Amount: int32(numeric)})
							log.Println(ack.Ack)
						}
					}
					//If response is stored break, else re-send
					_, foundUniqueRequest := p.requestsHandled[uniqueId]
					if foundUniqueRequest {
						break
					}
				}
			}
		}
	}
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
	//If primary fails, just promote highest id backup to primary.
	shouldBeRole := BACKUP
	highestClientSeen := int32(0)
	for i, _ := range p.clients {
		if i < p.id && p.id > highestClientSeen {
			shouldBeRole = PRIMARY
			highestClientSeen = p.id
		} else if i > p.id {
			highestClientSeen = i
			shouldBeRole = BACKUP
		}
	}
	p.replicationRole = shouldBeRole
	p.idOfPrimaryReplicationManager = highestClientSeen
	if len(p.clients) == 0 {
		p.replicationRole = PRIMARY
		p.idOfPrimaryReplicationManager = p.id
	}
	log.Printf("Replication role: %v \n", p.replicationRole)
}

func (p *peer) getUniqueIdentifier() (uniqueId int32) {
	uniqueIdentifier++
	return uniqueIdentifier + p.id
}

func (p *peer) openAuction(item string) {
	if p.auctionState == OPEN {
		return
	}
	p.auctionState = OPEN
	p.agreementsNeededFromBackups = int32(len(p.clients))
	p.highestBidOnCurrentAuction = 0
	announceAuction := "Auction for " + item + " is now open for 30 seconds! \nEnter Bid <amount> to bid on the item. \nEnter Result to get current highest bid."
	log.Println(announceAuction)
	p.SendMessageToAllPeers(announceAuction)
}

func (p *peer) closeAuction(item string) {
	p.auctionState = CLOSED
	p.agreementsNeededFromBackups = int32(len(p.clients))
	announceWinner := fmt.Sprintf("Auction for %s is Over! \nHighest bid was %v\nNext auction starts in 10 seconds", item, p.highestBidOnCurrentAuction)
	log.Println(announceWinner)
	p.SendMessageToAllPeers(announceWinner)
}

func (p *peer) getAgreementFromAllPeersAndReplicateLeaderData(ack string, identifier int32) (agreementReached bool) {
	p.agreementsNeededFromBackups = int32(len(p.clients))

	for id, client := range p.clients {
		_, err := client.HandleAgreementAndReplicationFromLeader(p.ctx, &node.Replicate{AuctionStatus: p.auctionState, HighestBidOnCurrentAuction: p.highestBidOnCurrentAuction, ResponseForRequest: ack, UniqueIdentifierForRequest: identifier})
		if err != nil {
			log.Printf("Client node %v is dead", id)
			//Remove dead node from list of clients
			delete(p.clients, id)
			//If leader crashed, elect new leader
			if id == p.idOfPrimaryReplicationManager {
				p.LookAtMeLookAtMeIAmTheCaptainNow()
			}
		}
		p.agreementsNeededFromBackups--

	}
	return p.agreementsNeededFromBackups == 0
}

func (p *peer) HandleAgreementAndReplicationFromLeader(ctx context.Context, replicate *node.Replicate) (*node.Acknowledgement, error) {
	//Replicate here. Here leader should send the data of itself.
	p.auctionState = replicate.AuctionStatus
	p.highestBidOnCurrentAuction = replicate.HighestBidOnCurrentAuction
	p.requestsHandled[replicate.UniqueIdentifierForRequest] = replicate.ResponseForRequest
	reply := &node.Acknowledgement{Ack: "OK"}
	return reply, nil
}

func (p *peer) sendHeartbeatPingToPeerId(peerId int32) {
	_, found := p.clients[peerId]
	if !found {
		log.Printf("Client node %v is dead", peerId)
		//Remove dead node from list of clients
		delete(p.clients, peerId)
		//If leader crashed, elect new leader
		if peerId == p.idOfPrimaryReplicationManager {
			p.LookAtMeLookAtMeIAmTheCaptainNow()
		}
		return
	}
	_, err := p.clients[peerId].PingLeader(p.ctx, &emptypb.Empty{})
	if err != nil {
		log.Printf("Client node %v is dead", peerId)
		//Remove dead node from list of clients
		delete(p.clients, peerId)
		//If leader crashed, elect new leader
		if peerId == p.idOfPrimaryReplicationManager {
			p.LookAtMeLookAtMeIAmTheCaptainNow()
		}
	}
}

func (p *peer) PingLeader(ctx context.Context, empty *emptypb.Empty) (*node.Acknowledgement, error) {
	reply := &node.Acknowledgement{Ack: "Alive"}
	return reply, nil
}

func (p *peer) SendMessageToAllPeers(message string) {
	for id, client := range p.clients {
		_, err := client.BroadcastMessage(p.ctx, &node.Acknowledgement{Ack: message})
		if err != nil {
			log.Printf("Client node %v is dead", id)
			//Remove dead node from list of clients
			delete(p.clients, id)
			//If leader crashed, elect new leader
			if id == p.idOfPrimaryReplicationManager {
				p.LookAtMeLookAtMeIAmTheCaptainNow()
			}
		}
	}
}

func (p *peer) BroadcastMessage(ctx context.Context, message *node.Acknowledgement) (*emptypb.Empty, error) {
	log.Println(message.Ack)
	return &emptypb.Empty{}, nil
}

func (p *peer) Bid(ctx context.Context, bid *node.Bid) (*node.Acknowledgement, error) {
	agreement := false
	acknowledgement := &node.Acknowledgement{}

	//If already processed, send same Ack.
	ack, found := p.requestsHandled[bid.UniqueBidId]
	if found {
		acknowledgement.Ack = ack
	}

	if p.auctionState == CLOSED && !found {
		acknowledgement.Ack = "Fail, auction is closed"
	}

	//If we reach this point, then we have reached agreement and can check on bid.
	if p.highestBidOnCurrentAuction < bid.Amount && !found {
		p.highestBidOnCurrentAuction = bid.Amount
		acknowledgement.Ack = "OK"
	} else if p.highestBidOnCurrentAuction >= bid.Amount && !found {
		acknowledgement.Ack = "Fail, your bid was too low"
	}

	p.requestsHandled[bid.UniqueBidId] = acknowledgement.Ack

	agreement = p.getAgreementFromAllPeersAndReplicateLeaderData(acknowledgement.Ack, bid.UniqueBidId)
	if !agreement {
		acknowledgement.Ack = "Fail, couldn't reach agreement in phase 4"
		p.requestsHandled[bid.UniqueBidId] = acknowledgement.Ack
	}

	return acknowledgement, nil

}

func (p *peer) Result(ctx context.Context, empty *emptypb.Empty) (*node.Outcome, error) {
	if p.auctionState == OPEN {
		return &node.Outcome{AuctionStatus: "Auction is OPEN", HighestBid: p.highestBidOnCurrentAuction}, nil
	} else {
		return &node.Outcome{AuctionStatus: "Auction is CLOSED", HighestBid: p.highestBidOnCurrentAuction}, nil
	}
}

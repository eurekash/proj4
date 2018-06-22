package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"time"
	"io/ioutil"
	"log"
	"math/rand"
	"strconv"
	"sync"

	pb "../protobuf/go"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

var nserver int
var address []string

func Init() {
	conf, err := ioutil.ReadFile("config.json")
	if err != nil {
		panic(err)
	}
	var dat map[string]interface{}
	err = json.Unmarshal(conf, &dat)
	if err != nil {
		panic(err)
	}
	nserver = int(dat["nservers"].(float64)) 
	for i:=1; i<=nserver; i++ {
		dati := dat[strconv.Itoa(i)].(map[string]interface{})
		address = append(address, fmt.Sprintf("%s:%s", dati["ip"], dati["port"]))
	}
}

var (
	OpCode = flag.String("T", "", `DB transactions to perform:
	1 (or GET):      Show the balance of a given UserID. 
		Require option -user.
	5 (or TRANSFER): Transfer some money from one account to another.
		Require option -from, -to, -value.
	// 6 (or VERIFY): ...
	`)
	UserID = flag.String("user", "00000000", "User account ID for the operation.")
	FromID = flag.String("from", "00000000", "From account (for Transfer)")
	ToID   = flag.String("to", "12345678", "To account (for Transfer)")
	Value  = flag.Int("value", 1, "Amount of transaction")
	Fee  = flag.Int("fee", 1, "Mining Fee of transaction")
)

func UUID128bit() string {
	// Returns a 128bit hex string, RFC4122-compliant UUIDv4
	u:=make([]byte,16)
	_,_=rand.Read(u)
	// this make sure that the 13th character is "4"
	u[6] = (u[6] | 0x40) & 0x4F
	// this make sure that the 17th is "8", "9", "a", or "b"
	u[8] = (u[8] | 0x80) & 0xBF 
	return fmt.Sprintf("%x",u)
}

var conn []*grpc.ClientConn
var trans []*pb.Transaction
var lock sync.RWMutex
var balance map[int]int
var nuser int
func test_get() {
	for i:=0; ; i++ {
		lock.RLock()
		end := len(trans)
		lock.RUnlock()
		if i >= end {  continue  }
		in := trans[i]
		from,_ := strconv.Atoi(in.GetFromID())
		to,_ := strconv.Atoi(in.GetToID())
		fmt.Printf("transaction: from = %d, to = %d\n",from,to)
		if balance[from] >= int(in.Value) {
			balance[from] -= int(in.Value)
			balance[to] += int(in.Value - in.MiningFee)
		}
		for j:=0; j<nserver; j++ {
			c := pb.NewBlockChainMinerClient(conn[j])
			for ;; {
				res, _ := c.Verify(context.Background(), in)
				if res.Result != pb.VerifyResponse_PENDING {
					break
				}
			}
			check := true
			for k:=0; k<nuser; k++ {
				ans, err := c.Get(context.Background(), &pb.GetRequest{UserID: fmt.Sprintf("%08d",k)} )
				if err != nil {
					check = false
					break
				} else {
					if int(ans.Value) != balance[k] {
						check = false
						break
					}
				}
			}
			log.Printf("check server %d: %t\n",j+1,check)
		}
		time.Sleep(time.Nanosecond * 10000) 
	}
}

func main() {
//	flag.Parse()
	balance = make(map[int]int)
	Init()
	lock := &sync.RWMutex{}
	rand.Seed(int64(time.Now().Nanosecond()))

	nuser = 100 

	for i:=0; i<nuser; i++ {
		balance[i]=1000
	}

	ntrans := 10000

	//fmt.Println(UUID128bit())
	for i:=0; i<nserver; i++ {
		fmt.Println(address[i])
		conni, _ := grpc.Dial(address[i], grpc.WithInsecure())
		conn = append(conn, conni)
	}

//	go test_get()

	for i:=0; i<ntrans; i++ {
		var from int
		var to int
		for ;; {
			from = rand.Intn(nuser)
			to   = rand.Intn(nuser)
			if from != to {
				break
			}
		}
		value := rand.Intn(100)+2
		miningfee := rand.Intn(value-1)+1
		fromid := fmt.Sprintf("%08d", from)
		toid := fmt.Sprintf("%08d", to)
		c := pb.NewBlockChainMinerClient(conn[0])

		uuid := UUID128bit()
		//fmt.Printf("uuid = %s, fromid = %s, toid = %s, value = %d, miningfee = %d\n",uuid,fromid,toid,value,miningfee)
		in := &pb.Transaction{Type:pb.Transaction_TRANSFER, UUID:uuid, FromID: fromid, ToID: toid, Value: int32(value), MiningFee: int32(miningfee)}
		//trans[uuid] = in
		lock.Lock()
		trans = append(trans, in)
		lock.Unlock()

		r, err := c.Transfer(context.Background(), in)
		if err != nil {
			log.Printf("TRANSFER Error: %v", err)
		} else {
			log.Printf("TRANSFER Return: %s", r)
		}
		time.Sleep(time.Nanosecond * 100000)
	}
	/*

	time.Sleep(time.Second * 10)
	for _,in:=range trans {
		//for i:=0; i<nserver; i++ {
		i:=0
			c := pb.NewBlockChainMinerClient(conn[i])
			r, err := c.Verify(context.Background(), in)
			if err != nil {
				log.Printf("Verify Error %v", err)
			} else {
				log.Printf("uuid = %s, Verify Return: %s, %s", in.GetUUID(), r.Result, r.BlockHash)
			}
			temp := in.UUID
			in.UUID = UUID128bit()
			r, err = c.Verify(context.Background(), in)
			if err != nil {
				log.Printf("Verify Error %v", err)
			} else {
				log.Printf("uuid = %s, Verify Return: %s, %s", in.GetUUID(), r.Result, r.BlockHash)
			}
			in.UUID = temp
		//}
	}
*/
	for i:=0; i<nserver; i++ {
		conn[i].Close()
	}
	// Set up a connection to the server.
	//new client
	/*
	c := pb.NewBlockChainMinerClient(conn)

	switch *OpCode {
	case "":
		log.Fatal("Please specify an operation to perform (-T [1-5]). Run with -help to see a list of supported operations.")
	default:
		log.Fatal("Unknown operation.")
	case "1", "GET":
		if r, err := c.Get(context.Background(), &pb.GetRequest{UserID: *UserID}); err != nil {
			log.Printf("GET Error: %v", err)
		} else {
			log.Printf("GET Return: %d", r.Value)
		}
	case "5", "TRANSFER":
		if r, err := c.Transfer(context.Background(), &pb.Transaction{
				Type:pb.Transaction_TRANSFER,
				UUID:UUID128bit(),
				FromID: *FromID, ToID: *ToID, Value: int32(*Value), MiningFee: int32(*Fee)}); err != nil {
			log.Printf("TRANSFER Error: %v", err)
		} else {
			log.Printf("TRANSFER Return: %s", r)
		}
	//case Verify, GetBlock, GetHeight omitted.
	}
*/	
}

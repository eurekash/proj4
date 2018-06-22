package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"flag"
	"os"
	"sync"
	"strconv"
	"strings"

	pb "../protobuf/go"
	
	"github.com/golang/protobuf/jsonpb"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"crypto/sha256"
	"errors"
	"time"
)

const maxBlockSize = 50
const InitHash = "0000000000000000000000000000000000000000000000000000000000000000"


type NetworkManager struct {
	nservers int
	dat     map[string] interface{}
	conn 	[]*grpc.ClientConn
	client  []pb.BlockChainMinerClient
	status	[]int
	self	int
	lock	sync.RWMutex
	connected  bool
}

type Spider struct {
	mark map[string] bool   //to avoid cycle
	client pb.BlockChainMinerClient
}

/* Block Operations */
type Block struct {
	pb.Block
	hash 	string
}

//server
type server struct{
	blocks  map[string] *Block
	transs map[string] *Block
	buffer map[string] *pb.Transaction
	bal map[string] int
	leaf *Block
	lock sync.RWMutex
	stop chan bool
}

var id=flag.Int("id",1,"Server's ID, 1<=ID<=NServers")
var Dat map[string] interface{}
var IDstr string
var IDint int
var sv *server
var nm *NetworkManager
var MyMinerID string

func NewBlock(m *pb.Block) *Block {
	b := &Block{}
	b.BlockID = m.GetBlockID()
	b.PrevHash = m.GetPrevHash()
	b.MinerID = m.GetMinerID()
	b.Nonce = m.GetNonce()
	b.Transactions = m.GetTransactions()
	b.hash = ""
	return b
}

func NewNetworkManager(dat_ map[string] interface{}, self_ int) *NetworkManager{
	M := &NetworkManager{dat: dat_}
	n := int(dat_["nservers"].(float64)) 
	M.nservers = n
	M.conn = make([]*grpc.ClientConn, n+1)
	M.client = make([]pb.BlockChainMinerClient, n+1)
	M.status = make([]int, n+1)
	M.self = self_
	for i:=1; i<=n; i++ {
		M.status[i] = 0
	}
	return M
}

func (sp *Spider) GetBlock(s *server, hash string) (*Block, bool) { 
	s.lock.RLock()
	prev, valid := s.blocks[hash]
	s.lock.RUnlock()
	if valid {
//		log.Printf("spider GetBlock(%s) returns true\n",hash)
		return prev, true
	}

	if sp.mark[hash] {
//		log.Printf("spider GetBlock(%s) returns false: cyclic\n",hash)
		return nil, false
	}

	sp.mark[hash] = true
	js, err := sp.client.GetBlock(context.Background(), &pb.GetBlockRequest{BlockHash: hash})


///		log.Printf("spider GetBlock(%s) json string = %s\n",hash,js.Json)
	if err != nil {  
//		log.Printf("spider GetBlock(%s) returns false: Get Block json error %v\n",hash,err)
		return nil, false 
	}
	temp := new(pb.Block)
	err = jsonpb.UnmarshalString(js.Json, temp)
	if err != nil { 
//		log.Printf("spider GetBlock(%s) returns false: Get Block json conv error\n",hash)
		return nil, false 
	}
	b := NewBlock(temp)
	_, valid = sp.GetBlock(s, b.PrevHash)
	if !valid { 
//		log.Printf("spider GetBlock(%s) returns false: Prev Block failed\n",hash)
		return nil, false 
	}

	if s.AddBlock(b) == -1 {  
//		log.Printf("spider GetBlock(%s) returns false: Invalid block\n",hash)
		return nil, false  
	}
//	log.Printf("spider GetBlock(%s) returns true\n",hash)
	return b, true
}

/*
func (M *NetworkManager) GetBlock(hash string) (*Block, bool) {
	c := make(chan *Block)
	for i:=1; i<=M.nservers; i++ {
		if i == M.self {
			continue
		}
		M.lock.RLock()
		status := M.status[i]
		cli := M.client[i]
		M.lock.RUnlock()

		if status == 1 {
			go func() {
				sp := &Spider{mark: make(map[string]bool), client:cli}
				b, ok := sp.GetBlock(hash)
				if ok {
					c <- b
				} else {
					c <- nil
				}
			}()
		}
	}
	for i:=1; i<M.nservers*2/3; i++ {
		b := <- c
		if b != nil {
			return b, true
		}
	}
	return nil, false
}
*/

/* Push Block Operations */

func (s *server) Init() {
	log.Println("server init()")
	for ;; {
		for i:=1; i<=nm.nservers; i++ {
			if i == nm.self { continue }
			go func(id int) {
				//fmt.Printf("get blocks from %d\n",id)
				nm.lock.RLock()
				status := nm.status[id]
				cli := nm.client[id]
				nm.lock.RUnlock()
				if status == 1 {
					res, err := cli.GetHeight(context.Background(), &pb.Null{})
					if err == nil {
						leaf_hash := res.LeafHash
						sp := &Spider{mark: make(map[string]bool), client:cli}
						sp.GetBlock(s,leaf_hash)
					}
				}
			}(i)
		}
		time.Sleep(time.Second * 10)
	}
}

func (M *NetworkManager) PushBlock(b *Block) { 
	str := b.ToString()

	for i:=1; i<=M.nservers; i++ {
		if i == M.self {
			continue
		}
		go func(id int) {
			for ;; {
				M.lock.RLock()
				status := M.status[id]
				client := M.client[id]
				M.lock.RUnlock()
				if status == 0 { continue }
				_,err := client.PushBlock(context.Background(), &pb.JsonBlockString{Json: str})
				if err == nil {
					break
				}
			}
		}(i)
	}
}

func (s *server) PushBlock(ctx context.Context, in *pb.JsonBlockString) (*pb.Null, error) {
	temp := new(pb.Block)
	err := jsonpb.UnmarshalString(in.Json, temp)

	if err != nil {
		log.Printf("PushBlock error! %v\n", err)
		return &pb.Null{}, err
	}

	b := NewBlock(temp)
	s.AddBlock(b)
	log.Printf("Rec Block:\n    PrevHash = %s\n        Hash = %s\n         Depth = %d\n         MinerID = %s\n         Length = %d\n",
	 b.GetPrevHash(), b.GetHash(), b.GetBlockID(),b.MinerID, len(b.Transactions))

	return &pb.Null{}, nil
}



func (M *NetworkManager) Connect() {
	M.connected = false
	for ;; {
		for i:=1; i<=M.nservers; i++ {
			if i == M.self { continue }

			M.lock.Lock()
			if M.status[i] == 1 {
				M.conn[i].Close()
			}

			dat := M.dat[strconv.Itoa(i)].(map[string]interface{})
			address := fmt.Sprintf("%s:%s", dat["ip"], dat["port"])
			conn, err := grpc.Dial(address, grpc.WithInsecure())

			if err != nil {
				M.status[i] = 0
			} else {
				M.status[i] = 1
				M.client[i] = pb.NewBlockChainMinerClient(conn)
			}
			M.conn[i] = conn
			M.lock.Unlock()
		}
		M.connected = true
		time.Sleep(time.Second)
	}
}


func GetHashString(String string) string {
	return fmt.Sprintf("%x", GetHashBytes(String))
}

func GetHashBytes(String string) [32]byte {
	return sha256.Sum256([]byte(String))
}

func CheckHash(Hash string) bool {
	return Hash[0:5] == "00000"
}


func (m *Block) ToString () string {
	temp := new (pb.Block)
	temp.BlockID = m.BlockID
	temp.PrevHash = m.PrevHash

	temp.Transactions = make([]*pb.Transaction, 0)
	for _, trans := range m.Transactions {
		temp.Transactions = append(temp.Transactions, trans)
	}

	temp.MinerID = m.MinerID
	temp.Nonce = m.Nonce

	result, err := (&jsonpb.Marshaler{EnumsAsInts: false}).MarshalToString(temp)

	if err != nil {
		fmt.Println("unable to transform from Marshal to String: ", err)
		os.Exit(1)
	}

	return result
}

func (m *Block) Height() int32 {
	return m.BlockID
}

func (m *Block) GetHash() string {
	if m.hash == "" {
		m.hash = GetHashString(m.ToString())
	}
	return m.hash
}

func (m *Block) GetTransactions() []*pb.Transaction {
	return m.Transactions
}
//Block operations end


func NewServer() *server {
	s := &server{blocks: make(map[string]*Block), 
		transs: make(map[string]*Block), 
		buffer: make(map[string] *pb.Transaction), 
		bal: make(map[string] int),
		leaf: nil,
		lock: sync.RWMutex{},
		stop: make(chan bool)}
	s.blocks[InitHash] = nil
    return s
}

func Add(bal map[string] int, id string, delta int) bool {
	val, valid := bal[id]
	if valid == false {
		val = 1000
	}
	val += delta
	bal[id] = val
	return val >= 0
}

func (s *server) move_leaf(b *Block) {
	p := s.leaf
	q := b
	for ; p != q; {
		if q == nil || p.Height() >= q.Height() {
			for _, trans := range p.GetTransactions() {
				Add(s.bal, trans.GetFromID(), int(trans.GetValue()))
				Add(s.bal, trans.GetToID(), int(trans.GetMiningFee() - trans.GetValue()))
				delete(s.transs, trans.GetUUID())
				s.buffer[trans.GetUUID()] = trans
			}
			p = s.blocks[p.PrevHash]
		} else {
			q = s.blocks[q.PrevHash]
		}
	}

	for q = b; q != p; {
		for _, trans := range q.GetTransactions() {
			Add(s.bal, trans.GetFromID(), int(-trans.GetValue()))
			Add(s.bal, trans.GetToID(), int(- trans.GetMiningFee() + trans.GetValue()))
			s.transs[trans.GetUUID()] = q
			delete(s.buffer, trans.GetUUID())
		}
		q = s.blocks[q.PrevHash]
	}
	s.leaf = b
}

func (s *server) check(b *Block,bal map[string]int, uu map[string]bool) bool {
	if b == nil { return true }
	if !s.check(s.blocks[b.PrevHash], bal, uu) {
		return false
	}
	for _,t := range b.Transactions {
		if uu[t.GetUUID()] { return false }
		uu[t.GetUUID()]=true
		if !Add(bal,t.GetFromID(),int(-t.GetValue())) { return false }
		Add(bal,t.GetToID(),int(t.GetValue()-t.GetMiningFee()))
	}
	return true
}

func (s *server) Extend(b *Block) bool {
	if b.PrevHash == InitHash {
		if s.leaf != nil {return false}
	} else if s.leaf == nil || b.PrevHash != s.leaf.GetHash() {
		return false
	}

	valid := true

	for _, trans := range b.GetTransactions() {
		_, exist := s.transs[trans.GetUUID()]
		if exist {
			valid = false
		}

		if !Add(s.bal, trans.GetFromID(), int(-trans.GetValue()) ){
			//fmt.Printf("Extend add failed %d %d\n", s.bal[trans.GetFromID()], trans.GetValue())
			valid = false
		}
		if !Add(s.bal, trans.GetToID(), int(trans.GetValue() - trans.GetMiningFee())){
			valid = false
		}
	}

	if valid == false {
		for _, trans := range b.GetTransactions() {
		 	Add(s.bal, trans.GetFromID(), int(trans.GetValue()) )
		 	Add(s.bal, trans.GetToID(), int( - trans.GetValue() + trans.GetMiningFee()) )
		}
		return false
	} else {
		for _, trans := range b.GetTransactions() {
			s.transs[trans.GetUUID()] = b
			delete (s.buffer, trans.GetUUID())
		}
		s.leaf = b
//		bal := make(map[string]int)
//		uu := make(map[string]bool)
//		if !s.check(b,bal,uu) {
//			log.Printf("check err!")
//		} else {
//			log.Printf("passed check!")
//		}
//		val_chk := true
//		for user,v := range bal {
//			if s.bal[user] != v {
//				val_chk = false
//			}
//		}
//		log.Printf("value check = %t", val_chk)
		return true
	}
}

func CheckServerID(s string) bool {
	if (len(s) != 8) {
		return false
	}
	if s[0] != 'S' || s[1] != 'e' || s[2] != 'r' || s[3] != 'v' || s[4] != 'e' || s[5] != 'r' {
		return false
	}
	_, err := strconv.Atoi(string(s[6:8]))
	return err == nil
}

func (s *server) AddBlock(b *Block) int {
	s.lock.Lock()
	defer s.lock.Unlock()


	log.Printf("Addblock: %s\n", b.GetHash())
	if CheckHash(b.GetHash()) == false {
		//log.Printf("Addblock return -1: hash incorrect!\n")
		return -1
	}

	if CheckServerID(b.MinerID) == false {
		return -1
	}

	_, exist := s.blocks[b.GetHash()]

	if exist == true {
		//log.Printf("Addblock return 0:  already exists!\n")
		return 0 
	}

	prev, exist_prev := s.blocks[b.PrevHash]

	if exist_prev == false {
		//log.Printf("Addblock return -1:  already exists!\n")
		return -1
	}

	current_tail := s.leaf
	if prev != nil {
		s.move_leaf(prev)
	}

	if s.Extend(b) == false {
		s.move_leaf(current_tail)
	//	log.Printf("Addblock return -1:  invalid extension!\n")
		return -1
	}

	s.blocks[b.GetHash()] = b
	if current_tail != nil && (current_tail.Height() > b.Height() || current_tail.Height() == b.Height() && current_tail.GetHash() < b.GetHash()) {
		s.move_leaf(current_tail)
	//	log.Printf("Addblock return 0: not longest!\n")
		return 0
	} 

	//log.Printf("Addblock return 1!\n")
	return 1
}

func valid_trans(in *pb.Transaction) bool {
	if in.GetMiningFee() <= 0 {return false }
	if in.GetValue() <= in.GetMiningFee() {return false }
 	if len(in.GetFromID()) != 8 {return false }
 	if len(in.GetToID()) != 8 {return false }
	return true
}

// Database Interface 
func (s *server) Get(ctx context.Context, in *pb.GetRequest) (*pb.GetResponse, error) {
	user_id := in.GetUserID()
	s.lock.RLock()
	defer s.lock.RUnlock()

	val, exist := s.bal[user_id]
	if exist == false {
		return &pb.GetResponse{Value: 1000}, nil
	}
	return &pb.GetResponse{Value: int32(val)}, nil
}

func (M *NetworkManager) PushTransaction(in *pb.Transaction) {
	for i:=1; i<=M.nservers; i++ {
		if i == M.self {
			continue
		}
		
		M.lock.RLock()
		status := M.status[i]
		client := M.client[i]
		M.lock.RUnlock()
		if status == 1 {
			client.PushTransaction(context.Background(), in)
		}
	}
}

func (s *server) Transfer(ctx context.Context, in *pb.Transaction) (*pb.BooleanResponse, error) {
	if !valid_trans(in) {
		return &pb.BooleanResponse{Success: false}, nil
	}
	s.lock.RLock()
	_, exist:= s.buffer[in.GetUUID()]
	if exist {
		s.lock.RUnlock()
		return &pb.BooleanResponse{Success: false}, nil
	}
	s.lock.RUnlock()
	s.lock.Lock()
	s.buffer[in.GetUUID()] = in
	s.lock.Unlock()
	//push transaction
	nm.PushTransaction(in)
	return &pb.BooleanResponse{Success: true}, nil
}

func (s *server) Verify(ctx context.Context, in *pb.Transaction) (*pb.VerifyResponse, error) {
	if valid_trans(in) == false {
		return &pb.VerifyResponse{Result: pb.VerifyResponse_FAILED, BlockHash:"?"}, errors.New("Verify: transaction is invalid.")
	}
	s.lock.RLock()
	defer s.lock.RUnlock()
	b, exist := s.transs[in.GetUUID()]

	if exist == false {
		return &pb.VerifyResponse{Result: pb.VerifyResponse_FAILED, BlockHash:"?"}, nil
	}

	if b.Height() + 6 > s.leaf.Height() {
		return &pb.VerifyResponse{Result: pb.VerifyResponse_PENDING, BlockHash:b.GetHash()}, nil
	}

	return &pb.VerifyResponse{Result: pb.VerifyResponse_SUCCEEDED, BlockHash:b.GetHash()}, nil
}

func (s *server) GetHeight(ctx context.Context, in *pb.Null) (*pb.GetHeightResponse, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	if s.leaf == nil {
		return &pb.GetHeightResponse{Height: 0, LeafHash: InitHash}, nil
	} else {
		return &pb.GetHeightResponse{Height: s.leaf.Height(), LeafHash: s.leaf.GetHash()}, nil
	}
}


func (s *server) GetBlock(ctx context.Context, in *pb.GetBlockRequest) (*pb.JsonBlockString, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	blk, valid := s.blocks[in.BlockHash] 
	if valid {
		return &pb.JsonBlockString{Json: blk.ToString()}, nil
	} 
	return &pb.JsonBlockString{Json: "{}"}, nil
}


func (s *server) PushTransaction(ctx context.Context, in *pb.Transaction) (*pb.Null, error) {
	if !valid_trans(in) {
		return &pb.Null{}, nil
	}
	s.lock.Lock()
	s.buffer[in.GetUUID()] = in
	s.lock.Unlock()
	return &pb.Null{}, nil
}

func (s *server) Produce() {
	s.lock.RLock()
	var b *Block
	if s.leaf == nil {
		b = NewBlock(
			&pb.Block{BlockID:1, PrevHash: InitHash, MinerID:MyMinerID, Nonce:"goosepig",Transactions:make([]*pb.Transaction,0)})
	} else {
		b = NewBlock(
			&pb.Block{BlockID:s.leaf.BlockID+1, PrevHash: s.leaf.GetHash(), 
				MinerID:MyMinerID, Nonce:"goosepig",Transactions:make([]*pb.Transaction,0)})
	}
	delta := make(map[string]int)
	for _, trans := range s.buffer {
		if len(b.GetTransactions()) >= 50 {
			break
		}
		delta[trans.GetFromID()] -= int(trans.GetValue())
		_, valid := s.bal[trans.GetFromID()]
		if valid == false {
			s.bal[trans.GetFromID()] = 1000
		}
		//fmt.Printf("ID %s: new value = %d\n", trans.GetFromID(), s.bal[trans.GetFromID()] + delta[trans.GetFromID()])  
		if s.bal[trans.GetFromID()] + delta[trans.GetFromID()] < 0 {
			delta[trans.GetFromID()] += int(trans.GetValue())
			continue
		}
		delta[trans.GetToID()] += int(trans.GetValue() - trans.GetMiningFee())
		b.Transactions = append(b.Transactions, trans)
	}
	s.lock.RUnlock()
	if len(b.GetTransactions()) == 0 {
		return 
	}

	log.Println("Producing Block")
	nchan := make(chan string)
	for st := 0; st < 10; st ++ {
		go func(first int) {
			str := b.ToString()
			index_nonce := strings.Index(str, "goosepig")
			arr := []byte(str)
			for nonce:=first*10000000; nonce<(first+1)*10000000; nonce++ {
				if (nonce & 0xfff) == 0 {
					s.lock.RLock()
					if s.leaf != nil && s.leaf.GetHash() != b.PrevHash {
						s.lock.RUnlock()
						nchan <- "goosepig"
						return
					}
					s.lock.RUnlock()
				}
				nonce_str := fmt.Sprintf("%08d", nonce)
				for i := 0; i < 8; i++ { arr[index_nonce+i] = nonce_str[i]}
				hash := GetHashString(string(arr))
				if CheckHash(hash) {
					nchan <- nonce_str
					break
				}
			}
		}(st)
	}
	for i:=0; i<10; i++ {
		b.Nonce = <- nchan
		if b.Nonce == "goosepig" { continue }
		log.Println("Block Produced")
		s.AddBlock(b)
		log.Printf("New Block:\n    PrevHash = %s\n        Hash = %s\n         Depth = %d\n         MinerID = %s\n         Length = %d\n", b.GetPrevHash(), b.GetHash(), b.GetBlockID(), b.MinerID,
			len(b.Transactions))
		nm.PushBlock(b)
		break
	}
}

// Main function, RPC server initialization
func main() {
	flag.Parse()
	IDstr := fmt.Sprintf("%d",*id)
	IDint := int(*id)
	MyMinerID = fmt.Sprintf("Server%02d", *id)
///	_=hash.GetHashString
	
	// Read config
	conf, err := ioutil.ReadFile("config.json")
	if err != nil {
		panic(err)
	}
	err = json.Unmarshal(conf, &Dat)
	if err != nil {
		panic(err)
	}
	dat := Dat[IDstr].(map[string]interface{}) // should be dat[myNum] in the future
	address, _ := fmt.Sprintf("%s:%s", dat["ip"], dat["port"]), fmt.Sprintf("%s",dat["dataDir"])

	// Bind to port
	lis, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	log.Printf("Listening: %s ...", address)

	nm = NewNetworkManager(Dat, IDint)
	fmt.Printf("nservers = %d\n", nm.nservers)
	go nm.Connect()

	// Create gRPC server
	grpc_s := grpc.NewServer()

	sv := NewServer()
	pb.RegisterBlockChainMinerServer(grpc_s, sv)
	// Register reflection service on gRPC server.
	reflection.Register(grpc_s)


	go sv.Init()
	go func() {
		for ; ; {
			sv.Produce()
			time.Sleep(time.Nanosecond * 100)
		}
	} ()
	// Start server
	if err := grpc_s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}

}

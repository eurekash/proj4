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

func (sp *Spider) GetBlock(hash string) (*Block, bool) {
	sv.lock.RLock()
	prev, valid := sv.blocks[hash]
	sv.lock.RUnlock()
	if valid {
		return prev, true
	}

	if sp.mark[hash] {
		return nil, false
	}

	sp.mark[hash] = true
	js, err := sp.client.GetBlock(context.Background(), &pb.GetBlockRequest{BlockHash: hash})

	if err != nil {  return nil, false }
	temp := new(pb.Block)
	err = jsonpb.UnmarshalString(js.Json, temp)
	if err != nil { return nil, false }
	b := NewBlock(temp)
	_, valid = sp.GetBlock(b.PrevHash)
	if !valid { return nil, false }
	if sv.AddBlock(b) == -1 {  return nil, false  }
	return b, true
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

func (M *NetworkManager) Connect() {
	M.connected = false
	for ;; {
		for i:=1; i<=M.nservers; i++ {
			if i == M.self {
				continue
			}

			M.lock.Lock()
			if M.status[i] == 1 {
				M.status[i] = 0
				M.conn[i].Close()
			}
			dat := M.dat[strconv.Itoa(i)].(map[string]interface{})
			address := fmt.Sprintf("%s:%s", dat["ip"], dat["port"])
			conn, err := grpc.Dial(address, grpc.WithInsecure())

			M.lock.Lock()
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
	}
}


func GetHashString(String string) string {
	return fmt.Sprintf("%x", GetHashBytes(String))
}

func GetHashBytes(String string) [32]byte {
	return sha256.Sum256([]byte(String))
}

func CheckHash(Hash string) bool {
	return Hash[0:5]=="00000"
}

const maxBlockSize = 50
const InitHash = "0000000000000000000000000000000000000000000000000000000000000000"


/* Block Operations */
type Block struct {
	pb.Block
	hash 	string
}

func NewBlock(m *pb.Block) *Block {
	b := &Block{}
	b.BlockID = m.GetBlockID()
	b.PrevHash = m.GetPrevHash()
	b.MinerID = m.GetMinerID()
	b.Nonce = m.GetNonce()
	b.Transactions = m.GetTransactions()
	return b
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

func (s *server) Init() {

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
		fmt.Println("invalid Extend")
		for _, trans := range b.GetTransactions() {
		 	Add(s.bal, trans.GetFromID(), int(trans.GetValue()) )
		 	Add(s.bal, trans.GetToID(), int( - trans.GetValue() + trans.GetMiningFee()) )
		}
		return false
	} else {
		fmt.Println("valid Extend")
		for _, trans := range b.GetTransactions() {
			s.transs[trans.GetUUID()] = b
			delete (s.buffer, trans.GetUUID())
		}
		s.leaf = b
		return true
	}
}

func (s *server) AddBlock(b *Block) int {
	s.lock.Lock()
	defer s.lock.Unlock()

	if CheckHash(b.GetHash()) == false {
		return -1
	}

	_, exist := s.blocks[b.GetHash()]

	if exist == true {
		return 0 
	}

	fmt.Printf("prev_hash = %s\n", b.PrevHash)
	prev, exist_prev := s.blocks[b.PrevHash]

	if exist_prev == false {
		return -1
	}

	current_tail := s.leaf
	if prev != nil {
		s.move_leaf(prev)
	}

	if s.Extend(b) == false {
		s.move_leaf(current_tail)
		return -1
	}

	s.blocks[b.GetHash()] = b
	if current_tail != nil && current_tail.Height() >= b.Height() {
		s.move_leaf(current_tail)
		return 0
	} 

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
	//fmt.Printf("Transfer(%s) %s -> %s, %d %d\n", in.GetUUID(), in.GetFromID(), in.GetToID(), in.GetValue(), in.GetMiningFee())
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
	//nm.PushTransaction(in)
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

func (s *server) GetBlock(ctx context.Context, in *pb.GetBlockRequest) (*pb.JsonBlockString, error) {
	s.lock.RLock()
	blk, valid := s.blocks[in.BlockHash] 
	if valid {
		s.lock.RUnlock()
		return &pb.JsonBlockString{Json: blk.ToString()}, nil
	} else {
		/*blk, valid = nm.GetBlock(in.BlockHash)
		if valid {
			return &pb.JsonBlockString{Json: blk.ToString()}, nil
		}
		*/
	}

	return &pb.JsonBlockString{Json: "{}"}, nil
}

func (s *server) PushBlock(ctx context.Context, in *pb.JsonBlockString) (*pb.Null, error) {
	temp := new(pb.Block)
	err := jsonpb.UnmarshalString(in.Json, temp)

	if err != nil {
		return &pb.Null{}, err
	}

	b := NewBlock(temp)
	if s.AddBlock(b) == 1 {
		s.stop <- true
	}

	return &pb.Null{}, nil
}

func (s *server) PushTransaction(ctx context.Context, in *pb.Transaction) (*pb.Null, error) {
	if !valid_trans(in) {
		return &pb.Null{}, nil
	}
	s.lock.RLock()
	_, exist:= s.buffer[in.GetUUID()]
	if exist {
		s.lock.RUnlock()
		return &pb.Null{}, nil
	}
	s.lock.RUnlock()
	s.lock.Lock()
	s.buffer[in.GetUUID()] = in
	s.lock.Unlock()
	return &pb.Null{}, nil
}

func (s *server) Produce() *Block {
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
		return nil
	}

	fmt.Printf("Producing..., prevhash = %s\n",b.PrevHash)
	str := b.ToString()
	index_nonce := strings.Index(str, "goosepig")
	arr := []byte(str)
	for nonce := 0; nonce < 99999999; nonce += 1 {
		if (nonce & (0x11111)) == 0 {    
			select {
				case flag := <- s.stop:
					if flag {
						return nil
					}
				case <- time.After(time.Nanosecond * 100):
			}
		}
		nonce_str := fmt.Sprintf("%08d", nonce)
		for i := 0; i < 8; i++ { arr[index_nonce+i] = nonce_str[i]}
		hash := GetHashString(string(arr))
		if CheckHash(hash) {
			b.Nonce = nonce_str
			break
		}
	}
	fmt.Println("Add Block!")
	s.AddBlock(b)
	fmt.Println("Finish!")
	//nm.PushBlock(b)

	return b
}



func (M *NetworkManager) PushBlock(b *Block) {
	str := b.ToString()

	for i:=1; i<=M.nservers; i++ {
		if i == M.self {
			continue
		}

		M.lock.RLock()
		status := M.status[i]
		client := M.client[i]
		M.lock.RUnlock()

		if status == 1 {
			client.PushBlock(context.Background(), &pb.JsonBlockString{Json: str})
		}
	}
}

var id=flag.Int("id",1,"Server's ID, 1<=ID<=NServers")
var Dat map[string] interface{}
var IDstr string
var IDint int
var sv *server
var nm *NetworkManager
var MyMinerID string
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
	sv.Init()

	pb.RegisterBlockChainMinerServer(grpc_s, sv)
	// Register reflection service on gRPC server.
	reflection.Register(grpc_s)


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

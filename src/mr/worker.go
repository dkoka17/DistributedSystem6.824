package mr

import "log"
import "net/rpc"
import "hash/fnv"
import "sort"
import "os"
import "io/ioutil"
import "fmt"
import "encoding/json"
import "crypto/rand"
import "strconv"
import "sync"
import "time"

type maped []KeyValue

func (obj maped) Len() int{ 
    return len(obj) 
}
func (obj maped) Swap(fr, sc int){ 
    obj[fr], obj[sc] = obj[sc], obj[fr] 
}
func (obj maped) Less(fr, sc int) bool{
     return obj[fr].Key < obj[sc].Key 
}

type iterators struct {
    fr int
    sc int
}
type KeyValue struct {
    Key   string
    Value string
}


func hasher(txt string) int {
    gener := fnv.New32a()
    gener.Write([]byte(txt))
    return int(gener.Sum32() & 0x7fffffff)
}
var (
    waiters sync.WaitGroup
    s =&service{}
)

type service struct {
    currentWorker *worker
    w chan *worker
    ReduceNumb int
    MapFinishBool bool
    AllFinishBool bool

    mapf func(string, string) []KeyValue
    reducef func(string, []string) string
}

func (s *service) Sync(){
    if s.currentWorker==nil{
        s.Reset()
        return
    }
    args:=Args{Worker: s.currentWorker}
    reply:=Reply{}
    if !call("Master.Sync",&args,&reply) {
        os.Exit(0)
    }
    if reply.NextWorker!=nil{
        s.currentWorker=reply.NextWorker
        s.w<-reply.NextWorker
    }
    s.ReduceNumb=reply.ReduceNumb
    s.AllFinishBool=reply.AllFinishBool
    s.MapFinishBool=reply.MapFinishBool
    if reply.AllFinishBool{
        waiters.Done()
    }
}

func (s *service) Reset()  {
    s.currentWorker.Task=nil
    s.currentWorker.Status="inited"
    s.currentWorker.TaskTimeout=time.Time{}
}

func (s *service) Init(mapf func(string, string) []KeyValue,
    reducef func(string, []string) string)  {
    s.w=make(chan *worker)
    s.currentWorker =&worker{
        UUID:   uidGenerator(),
        Status: "inited",
        Task:   nil,
    }
    s.mapf=mapf
    s.reducef=reducef
}

func (s *service) Reduce(){
    t:=s.currentWorker.Task
    var intermediate []KeyValue
    intermediateFile,_:=os.Open(t.File)
    dec := json.NewDecoder(intermediateFile)
    for {
        var kv KeyValue
        if err := dec.Decode(&kv); err != nil {
            break
        }
        intermediate = append(intermediate, kv)
    }
    sort.Sort(maped(intermediate))

    localCache:="[inter-tmp]"+t.File
    file,err:=os.OpenFile(localCache,os.O_WRONLY|os.O_TRUNC, os.ModeAppend)
    defer func(){ _ = file.Close() }()
    iter:=iterators{}
    if err!=nil && os.IsNotExist(err) {
        file, _ = os.Create(localCache)
        iter.fr=0
        iter.sc=0
    } else {
        dec := json.NewDecoder(file)
        _ = dec.Decode(&iter)
    }
    for iter.fr<len(intermediate) {
        encoder:=json.NewEncoder(file)
        _ = encoder.Encode(&iter)
        var values []string

        iter.sc=iter.fr+1
        for iter.sc < len(intermediate) && intermediate[iter.sc].Key == intermediate[iter.fr].Key {
            iter.sc++
        }
        for k := iter.fr; k < iter.sc; k++ {
            values = append(values, intermediate[k].Value)
        }
        output := s.reducef(intermediate[iter.fr].Key, values)

        s:="mr-out-"+strconv.Itoa(hasher(intermediate[iter.fr].Key)%s.ReduceNumb+1)
        outputFile, err := os.OpenFile(s,os.O_APPEND|os.O_WRONLY, os.ModeAppend)
        if err!=nil{
            log.Print(err)
        }
        _, e := fmt.Fprintf(outputFile, "%v %v\n", intermediate[iter.fr].Key, output)
        if e!=nil{
            log.Print(e)
        }
        _ = outputFile.Close()
        iter.fr = iter.sc
    }
}

func (s *service) Map(){
    t:=s.currentWorker.Task
    file, err := os.Open(t.File)
    if err != nil {
        log.Fatalf("cannot open %v", file)
    }
    content, err := ioutil.ReadAll(file)
    if err != nil {
        log.Fatalf("cannot read %v", file)
    }
    _ = file.Close()
    kva:=s.mapf(t.File,string(content))
    for _, kv := range kva {
        s:="mr-tmp-map-"+strconv.Itoa(hasher(kv.Key)% s.ReduceNumb+1)
        outputFile, err := os.OpenFile(s,os.O_APPEND|os.O_WRONLY, os.ModeAppend)
        if err!=nil{
            log.Print(err)
        }
        encoder:=json.NewEncoder(outputFile)
        e:= encoder.Encode(&kv)
        if e!=nil{
            log.Print(e)
        }
        _ = outputFile.Close()
    }
}

func (s *service) ThreadStarter(){
    go func() {
        for {
            s.Sync()
            time.Sleep(time.Second)
        }
    }()
}

func uidGenerator() (uuid string){
    unit32Gen := uint32(time.Now().UTC().Unix())
    buffers := make([]byte, 12)
    numReader, err := rand.Read(buffers)
    if numReader != len(buffers) {
        panic(err)
    }
    if err != nil {
        panic(err)
    }
    return fmt.Sprintf("%x-%x-%x-%x-%x-%x\n", unit32Gen, buffers[0:2], buffers[2:4], buffers[4:6], buffers[6:8], buffers[8:])
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
    reducef func(string, []string) string) {

    s.Init(mapf,reducef)
    waiters.Add(1)
    s.ThreadStarter()
    go func() {
        for worker:=range s.w{
            waiters.Add(1)
            if t:=worker.Task;t!=nil {
                switch s.currentWorker.Task.Action {
                case "rdc":
                    for {
                        if !s.MapFinishBool {
                            time.Sleep(time.Second)
                        }else{
                            break
                        }
                    }
                    s.Reduce()
                    break
                case "map":
                    s.Map()
                    break
                }
                s.currentWorker.Status="Finished"
            }
            waiters.Done()
        }
    }()
    waiters.Wait()
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
/*
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}
*/
//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
    // c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
    sockname := masterSock()
    c, err := rpc.DialHTTP("unix", sockname)
    if err != nil {
        log.Fatal("dialing:", err)
    }
    defer c.Close()

    err = c.Call(rpcname, args, reply)
    if err == nil {
        return true
    }

    fmt.Println(err)
    return false
}
package mr

import "net"
import "os"
import "net/rpc"
import "net/http"
import "errors"
import "log"
import "strconv"
import "sync"
import "time"

type Master struct {
    preparedWork bool
    activeWorkers map[string]*worker
    taskSched []task
    wokerOnMap int
    wokerOnReduce int

    taskOnMap int
    taskOnReduce int
    nReduce int
    mutex sync.RWMutex
}

func (m *Master) Init(files []string, nReduce int) {
    m.activeWorkers = map[string]*worker{}
    m.taskSched=[]task{}
    m.nReduce=nReduce
    m.InitStatus()
    m.CreateLocalFiles(files,nReduce)
    m.preparedWork=true
}
func (m *Master) taskAdder(action string,files ...string) {
    var taskSchd []task
    for _,file:=range files {
        taskSchd=append(taskSchd,task{
            Action: action,
            File:   file,
        })
    }
    m.mutex.Lock()
    switch action {
        case "map":
            m.taskSched=append(taskSchd,m.taskSched...)
        break
        case "rdc":
            m.taskSched=append(m.taskSched,taskSchd...)
        break
    }
    m.mutex.Unlock()
    m.ReorderStatuses()
}
func (m *Master) ReorderStatuses(){
    go func() {
        m.InitStatus()
        m.mutex.Lock()
        for _,w:=range m.activeWorkers{
            if w.Task !=nil{
                switch w.Task.Action {
                case "map":
                    m.wokerOnMap+=1
                    break
                case "rdc":
                    m.wokerOnReduce+=1
                    break
                }
            }
        }
        for _,t:=range m.taskSched{
            switch t.Action {
            case "map":
                m.taskOnMap+=1
                break
            case "rdc":
                m.taskOnReduce+=1
                break
            }
        }
        m.mutex.Unlock()
    }()
}
func (m *Master) InitStatus()  {
    m.mutex.Lock()
    m.taskOnReduce=0
    m.wokerOnReduce=0
    m.taskOnMap=0
    m.wokerOnMap=0
    m.mutex.Unlock()
}
func (m *Master) CreateLocalFiles(files []string, ind int)  {
    var tmpMaps []string
    for i := 0;i != ind; i++ {
        output :="mr-out-"+strconv.Itoa(i+1)
        tmpMap :="mr-tmp-map-"+strconv.Itoa(i+1)
        tmpMaps=append(tmpMaps,tmpMap)
        if _,e:=os.Create(output);e!=nil{
            log.Printf("[Master] Create [%s] File Success",output)
        }
        if _,e:=os.Create(tmpMap);e!=nil{
            log.Printf("[Master] Create [%s] File Success",tmpMap)
        }
    }
    m.taskAdder("map",files...)
    m.taskAdder("rdc",tmpMaps...)

}
//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) AuthMaster(worker *worker) error  {
    if time.Now().After(worker.TaskTimeout) {
        return errors.New("Master timeout ")
    }
    return  nil
}

func (m *Master) RemoveWorkerFromMaster(worker *worker) {
    m.mutex.Lock()
    if _,ok:=m.activeWorkers[worker.UUID];ok{
        delete(m.activeWorkers,worker.UUID)
    }
    m.mutex.Unlock()
    m.ReorderStatuses()

}

func (m *Master) RemoveTaskFromWorker(task *task) {
    if task ==nil {
        return
    }
    m.mutex.Lock()
    for i,t:=range m.taskSched{
        if t.File==task.File &&t.Action==task.Action{
            m.taskSched=append(m.taskSched[0:i],m.taskSched[i+1:]...)
        }
    }
    m.mutex.Unlock()
    m.ReorderStatuses()
}

func (m *Master) AddActiveWorkerAndAssignATask(currentWorker *worker) (NextWorker *worker) {
    NextWorker=nil
    m.mutex.Lock()
    if len(m.taskSched)>0{
        t:=m.taskSched[0]
        m.taskSched=m.taskSched[1:]
        wokrerTmp:=worker{
            UUID:        currentWorker.UUID,
            Status:      "InProgress",
            Task:        &t,
            TaskTimeout: time.Now().Add(time.Second*10),
        }
        m.activeWorkers[currentWorker.UUID]=&wokrerTmp
        NextWorker=&wokrerTmp
    }
    m.mutex.Unlock()
    m.ReorderStatuses()
    return
}
func (m *Master) Sync(args *Args,reply *Reply) error{

    switch args.Worker.Status {
        case "inited":
        reply.NextWorker=
            m.AddActiveWorkerAndAssignATask(args.Worker)
        break
        case "InProgress":
            if e:=m.AuthMaster(args.Worker) ;e!=nil{
                return e
            }
        break
        case "Finished":
            m.RemoveWorkerFromMaster(args.Worker)
            m.RemoveTaskFromWorker(args.Worker.Task)
            reply.NextWorker=&worker{
                UUID:        args.Worker.UUID,
                Status:      "inited",
                TaskTimeout: time.Time{},
                Task:        nil,
            }
        break
    }
    reply.ReduceNumb=m.nReduce
    reply.MapFinishBool=false
    reply.AllFinishBool=false
    m.mutex.Lock()
    if m.taskOnMap==0&&m.wokerOnMap==0&&m.preparedWork{
        reply.MapFinishBool=true
    }
    if m.wokerOnMap==0&&m.taskOnMap==0&&m.taskOnReduce==0&&m.wokerOnReduce==0&&m.preparedWork{
        reply.AllFinishBool=true
    }
    m.mutex.Unlock()
    return nil
}

func (m *Master) Checker (){
    go func() {
        for{
            log.Printf("[Master] [Checker] Map Workers %d,Map Todo Tasks %d,Reduce Workers %d,Reduce Todo Tasks %d",
                m.wokerOnMap,m.taskOnMap,m.wokerOnReduce,m.taskOnReduce)
           	m.mutex.Lock()
            for _,w:=range m.activeWorkers{
                if time.Now().After(w.TaskTimeout){
                    go m.RemoveWorkerFromMaster(w)
                    go m.taskAdder(w.Task.Action,w.Task.File)
                }
            }
            m.mutex.Unlock()
            time.Sleep(time.Second)
        }

    }()
}


func (m *Master) server() {
    rpc.Register(m)
    rpc.HandleHTTP()
    //l, e := net.Listen("tcp", ":1234")
    sockname := masterSock()
    os.Remove(sockname)
    l, e := net.Listen("unix", sockname)
    if e != nil {
        log.Fatal("listen error:", e)
    }
    go http.Serve(l, nil)
}


func (m *Master) Done() bool {
    ret := false
    if m.wokerOnMap==0&&m.taskOnMap==0&&m.taskOnReduce==0&&m.wokerOnReduce==0&&m.preparedWork{
        ret=true
    }
    return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
    m := Master{}
    m.Init(files,nReduce)
    m.Checker()
    m.server()
    return &m
}
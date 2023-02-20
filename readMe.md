
How to run

1. Go to src/main, start the coordinator to listen to worker (func server).  
   go run mrcoordinator.go pg*.txt

2. Open another terminal, go to src/main, start a worker to ask coordinator for a task. 
   go build -buildmode=plugin ../mrapps/wc.go
   go run mrworker.go wc.so

3. Build our coordinator (MakeCoordinator) for 1 to call upon. Once it gets called, it will grab the files, and create a task for each file.

Workflow (Design)

src/mr/worker

Start a worker process. It will run func Worker (called by mr/worker.go when start). This will send a RPC request to coordinator to ask for a task.

In coordinator.go, create a tasks and assign the task once it gets a request. Design tasks to have different state (Map, Reduce, Exit, Wait)
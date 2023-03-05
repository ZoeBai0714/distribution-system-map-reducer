
How to run

1. Go to src/main, start the coordinator to listen to worker (func server).  
   go run mrcoordinator.go pg*.txt

2. Build our coordinator (MakeCoordinator) for 1 to call upon. Once it gets called, it will grab the files, and create a task for each file. Now file has 
   TaskStatus as Map

3. create a catchTimeout func to check if there's any task gets blocked(more than 10 seconds), if so put it back to the queue to have another worker work on it.

4. Open another terminal, go to src/main, start a worker (can open multiple worker in multiple terminals) to ask coordinator for a task. 
   go build -buildmode=plugin ../mrapps/wc.go
   go run mrworker.go wc.so

5. Start to build worker that controls mapper and reducer. Once it gets a task from #4, check which status is the task in and act coordingly.
   It's first Map, this will call mapper function to read each file task, and then for each file task, slot theem in NReducer by word(using iHash function pre-defined) After done we write it to local file and assign it to intermediate. 
   
   Then map job is now complete, let tell coordinatpr about it with func TaskCompleted through rpc. Once all the task files finished the mapping job, the taskState will go into next phase Reduce.

   Reduce will combine the slots back to a file


Workflow (Design)

src/mr/worker

Start a worker process. It will run func Worker (called by mr/worker.go when start). This will send a RPC request to coordinator to ask for a task.

In coordinator.go, create a tasks and assign the task once it gets a request. Design tasks to have different state (Map, Reduce, Exit, Wait)
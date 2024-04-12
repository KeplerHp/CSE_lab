#include <string>
#include <vector>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <mutex>

#include "map_reduce/protocol.h"

namespace mapReduce
{
    TaskInfo Coordinator::askTask(int i)
    {
        // Lab4 : Your code goes here.
        // Free to change the type of return value.
        if (reduce_task_cnt > 0)
        {
            // all is over, finish the map reduce
            return {-1, mr_tasktype::NONE, -1, n_reducer};
        }
        if (map_task_cnt < files.size())
        {
            // do map work
            auto index = map_task_cnt++;
            return {index, mr_tasktype::MAP, files[index], n_reducer};
        }
        if (!map_done)
        {
            // map has been assigned, but not joined
            return {0, mr_tasktype::NONE, 0, n_reducer};
        }
        // return reduce
        reduce_task_cnt++;
        return {0, mr_tasktype::REDUCE,
                static_cast<int>(files.size()), n_reducer};
    }

    int Coordinator::submitTask(int taskType, int index)
    {
        // Lab4 : Your code goes here.
        // std::unique_lock Lock(mtx);
        if (taskType == MAP)
        {
            map_done_cnt++;
            if (map_done_cnt >= files.size())
            {
                map_done = true;
            }
        }
        if (taskType == REDUCE)
        {
            printf("down\n");
            std::cout.flush();
            this->isFinished = true;
        }
        return 0;
    }

    // mr_coordinator calls Done() periodically to find out
    // if the entire job has finished.
    bool Coordinator::Done()
    {
        std::unique_lock uniqueLock(this->mtx);
        return this->isFinished;
    }

    // create a Coordinator.
    // nReduce is the number of reduce tasks to use.
    Coordinator::Coordinator(MR_CoordinatorConfig config,
                             const std::vector<std::string> &files, int nReduce)
    {
        this->files = files;
        this->isFinished = false;
        this->n_reducer = nReduce;
        // Lab4: Your code goes here (Optional).

        rpc_server = std::make_unique<
            chfs::RpcServer>(config.ip_address, config.port);
        rpc_server->bind(ASK_TASK, [this](int i)
                         { return this->askTask(i); });
        rpc_server->bind(SUBMIT_TASK, [this](int taskType, int index)
                         { return this->submitTask(taskType, index); });
        rpc_server->run(true, 1);
    }
}
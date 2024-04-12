#include <string>
#include <utility>
#include <vector>
#include <mutex>
#include "librpc/client.h"
#include "librpc/server.h"
#include "distributed/client.h"

// Lab4: Free to modify this file

namespace mapReduce
{
    enum mr_tasktype
    {
        NONE = 0,
        MAP,
        REDUCE
    };

    struct TaskInfo
    {
        int index;
        int type;
        int nfiles;

        std::string file_name;
        int n_worker;
        int n_reducer;
        MSGPACK_DEFINE(index, type, nfiles, file_name, n_reducer)

        TaskInfo()
        {
        }

        TaskInfo(const int i, const int t, const int n, const int r)
            : index(i), type(t), nfiles(n), n_reducer(r)
        {
        }

        TaskInfo(const int i, const int t, const std::string &name, const int r)
            : index(i), type(t), file_name(name), n_reducer(r)
        {
        }
    };

    struct KeyVal
    {
        KeyVal(const std::string &key, const std::string &val)
            : key(key), val(val)
        {
        }

        KeyVal()
        {
        }

        std::string key;
        std::string val;
    };

    std::vector<std::string> Strip(const std::string &word);

    std::vector<KeyVal> Map(const std::string &content);

    std::string Reduce(const std::string &key,
                       const std::vector<std::string> &values);

    const std::string ASK_TASK = "ask_task";
    const std::string SUBMIT_TASK = "submit_task";

    struct MR_CoordinatorConfig
    {
        uint16_t port;
        std::string ip_address;
        std::string resultFile;
        std::shared_ptr<chfs::ChfsClient> client;

        MR_CoordinatorConfig(std::string ip_address, uint16_t port,
                             std::shared_ptr<chfs::ChfsClient> client,
                             std::string resultFile)
            : port(port), ip_address(std::move(ip_address)),
              resultFile(resultFile), client(std::move(client))
        {
        }
    };

    class SequentialMapReduce
    {
    public:
        SequentialMapReduce(std::shared_ptr<chfs::ChfsClient> client,
                            const std::vector<std::string> &files,
                            std::string resultFile);
        void doWork();

    private:
        std::shared_ptr<chfs::ChfsClient> chfs_client;
        std::vector<std::string> files;
        std::string outPutFile;
    };

    class Coordinator
    {
    public:
        Coordinator(MR_CoordinatorConfig config,
                    const std::vector<std::string> &files, int nReduce);
        TaskInfo askTask(int);
        int submitTask(int taskType, int index);
        bool Done();

    private:
        int n_reducer;
        bool map_done = false;
        int map_done_cnt = 0, map_task_cnt = 0;
        int reduce_task_cnt = 0;
        std::vector<std::string> files;
        std::mutex mtx;
        bool isFinished;
        std::unique_ptr<chfs::RpcServer> rpc_server;
    };

    class Worker
    {
    public:
        explicit Worker(MR_CoordinatorConfig config);
        void doWork();
        void stop();

    private:
        void doMap(int index, const std::string &filename);
        void doReduce(int index, int nfiles);
        void doSubmit(mr_tasktype taskType, int index);
        void doReduceAll(int nfiles);

        int n_reducer;
        std::string outPutFile;
        std::unique_ptr<chfs::RpcClient> mr_client;
        std::shared_ptr<chfs::ChfsClient> chfs_client;
        std::unique_ptr<std::thread> work_thread;
        bool shouldStop = false;
    };
}
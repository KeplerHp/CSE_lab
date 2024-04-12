#include <iostream>
#include <fstream>
#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <mutex>
#include <string>
#include <thread>
#include <vector>
#include <unordered_map>

#include "map_reduce/protocol.h"

namespace mapReduce
{
    Worker::Worker(MR_CoordinatorConfig config)
    {
        mr_client = std::make_unique<chfs::RpcClient>(config.ip_address, config.port,
                                                      true);
        outPutFile = config.resultFile;
        chfs_client = config.client;
        work_thread = std::make_unique<std::thread>(&Worker::doWork, this);
        // Lab4: Your code goes here (Optional).
    }

    void Worker::doMap(int index, const std::string &filename)
    {
        auto mk_res = chfs_client->mknode(chfs::ChfsClient::FileType::REGULAR, 1,
                                          "map-" + std::to_string(index));
        if (mk_res.is_err())
        {
            assert(0);
        }
        auto node_id = mk_res.unwrap();
        auto look_res = chfs_client->lookup(1, filename);
        if (look_res.is_err())
            assert(0);
        auto file_id = look_res.unwrap();
        auto attr_res = chfs_client->get_type_attr(file_id);
        auto buffer = chfs_client->read_file(file_id, 0,
                                             attr_res.unwrap().second.size)
                          .unwrap();
        std::string content;
        content.assign(buffer.begin(), buffer.end());
        auto map_res = Map(content);
        std::stringstream ss;
        for (const auto &[key, val] : map_res)
        {
            ss << key << ' ' << val << ' ';
        }
        content = ss.str();
        buffer = {content.begin(), content.end()};
        chfs_client->write_file(node_id, 0, buffer);
        doSubmit(MAP, index);
    }

    void Worker::doReduce(int index, int nfiles)
    {
        std::map<std::string, std::vector<std::string> > reduce_map;
        std::stringstream res_ss;

        for (auto cnt = 0; cnt < nfiles; ++cnt)
        {
            std::string file_name = "map-" + std::to_string(cnt);
            auto node_id = chfs_client->lookup(1, file_name).unwrap();
            auto attr_res = chfs_client->get_type_attr(node_id);
            auto content = chfs_client->read_file(node_id, 0, attr_res.unwrap().second.size).unwrap();

            std::stringstream ss(std::string(content.begin(), content.end()));
            while (ss)
            {
                std::string key, value;
                ss >> key >> value;
                if (!key.empty() && !value.empty())
                {
                    reduce_map[key].emplace_back(value);
                }
            }
        }

        for (const auto &[key, vals] : reduce_map)
        {
            long num = std::accumulate(vals.begin(), vals.end(), 0L, [](long acc, const std::string &val)
                                       { return acc + std::stol(val); });
            res_ss << key << " " << num << "\n";
        }

        std::string res_str = res_ss.str();
        std::vector<chfs::u8> buffer(res_str.begin(), res_str.end());

        auto node_id = chfs_client->lookup(1, outPutFile).unwrap();
        chfs_client->write_file(node_id, 0, buffer);

        doSubmit(REDUCE, index);
    }

    void Worker::doSubmit(mr_tasktype taskType, int index)
    {
        mr_client->call("submit_task", static_cast<int>(taskType), index);
    }

    void Worker::doReduceAll(int nfiles)
    {
        const auto output_node_id = chfs_client->lookup(1, outPutFile).unwrap();
        std::vector<chfs::u8> all_content;
        for (int index = 0; index < nfiles; ++index)
        {
            const auto node_id = chfs_client->lookup(1, "reduce-" + std::to_string(index)).unwrap();
            const auto attr = chfs_client->get_type_attr(node_id).unwrap();
            auto content = chfs_client->read_file(node_id, 0, attr.second.size).unwrap();
            auto it = std::find(content.begin(), content.end(), '\0');
            printf("%ld, %ld\n", std::distance(it, content.begin()),
                   std::distance(content.end(), content.begin()));
            all_content.insert(all_content.end(), content.begin(), it);
        }
        chfs_client->write_file(output_node_id, 0, all_content);
    }

    void Worker::stop()
    {
        shouldStop = true;
        work_thread->join();
    }

    void Worker::doWork()
    {
        int i = 0;
        while (!shouldStop)
        {
            // Lab4: Your code goes here.
            auto info = mr_client->call("ask_task", i++).unwrap()->as<TaskInfo>();
            this->n_reducer = info.n_reducer;
            if (info.type == NONE)
            {
                if (info.index == -1)
                {
                    return;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
                continue;
            }
            if (info.type == MAP)
            {
                doMap(info.index, info.file_name);
                continue;
            }
            if (info.index == -1)
            {
                doReduceAll(n_reducer);
                continue;
            }
            doReduce(info.index, info.nfiles);
        }
    }
}
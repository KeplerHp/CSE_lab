#include <string>
#include <utility>
#include <vector>
#include <algorithm>
#include <sstream>

#include "map_reduce/protocol.h"

namespace mapReduce
{
    SequentialMapReduce::SequentialMapReduce(
        std::shared_ptr<chfs::ChfsClient> client,
        const std::vector<std::string> &files_, std::string resultFile)
    {
        chfs_client = std::move(client);
        files = files_;
        outPutFile = resultFile;
        // Your code goes here (optional)
    }

    void SequentialMapReduce::doWork()
    {
        std::map<std::string, long> map_res;

        for (const auto &file : files)
        {
            auto inode_id = chfs_client->lookup(1, file).unwrap();
            auto attr_res = chfs_client->get_type_attr(inode_id);
            auto content = chfs_client->read_file(inode_id, 0, attr_res.unwrap().second.size).unwrap();

            auto ret = Map(std::string(content.begin(), content.end()));

            for (const auto &[key, val] : ret)
            {
                map_res[key] += std::stol(val);
            }
        }

        std::vector<chfs::u8> buffer;
        buffer.reserve(chfs::DiskBlockSize);

        for (const auto &[word, times] : map_res)
        {
            std::string str = word + ' ' + std::to_string(times) + '\n';
            buffer.insert(buffer.end(), str.begin(), str.end());
        }

        auto inode_id = chfs_client->lookup(1, outPutFile).unwrap();
        chfs_client->write_file(inode_id, 0, buffer);
    }

}
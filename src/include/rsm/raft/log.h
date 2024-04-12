#pragma once

#include "common/macros.h"
#include "block/manager.h"
#include <mutex>
#include <vector>
#include <cstring>
#include <fstream>

namespace chfs {

/** 
 * RaftLog uses a BlockManager to manage the data..
 */
template <typename Command>
class RaftLog {
public:
    RaftLog(std::shared_ptr<BlockManager> bm, int id);
    ~RaftLog();
    void persist_log(int, Command &);
    void writelogs(std::vector<std::pair<int, Command>> &);
    void recover(std::vector<std::pair<int,Command>> &log, int &current_term,int &commit_index,int &last_applied ,int &vote_for);
    void updatemeta(int current_term, int commit_index, int last_applied, int vote_for=-1);
    void save_snap(int last_snap_idx, int last_snap_term, std::vector<u8> &data);
    void read_snap(int &last_snap_idx, int &last_snap_term, std::vector<u8> &data);

    /* Lab3: Your code here */

private:
    std::shared_ptr<BlockManager> bm_;
    std::mutex mtx;
    /* Lab3: Your code here */
    std::string dir_;
    std::string log_;
    std::string meta_;
    std::string snap_;
    std::shared_ptr<BlockManager> log_bm;
    std::shared_ptr<BlockManager> meta_bm;
    std::shared_ptr<BlockManager> snap_bm;

};

template <typename Command>
RaftLog<Command>::RaftLog(std::shared_ptr<BlockManager> bm, int id)
{
    /* Lab3: Your code here */
    dir_ = "/tmp/raft_log/";
    meta_ = dir_ + "meta.log" + std::to_string(id);
    log_ = dir_ + "entry.log" + std::to_string(id);
    snap_ = dir_ + "snap.log" + std::to_string(id);
    log_bm = std::make_shared<BlockManager>(log_);
    meta_bm = std::make_shared<BlockManager>(meta_);
    snap_bm = std::make_shared<BlockManager>(snap_);
}

template <typename Command>
RaftLog<Command>::~RaftLog()
{
    /* Lab3: Your code here */
}

/* Lab3: Your code here */
template <typename Command>
void RaftLog<Command>::persist_log(int term, Command &cmd) {
    // std::unique_lock<std::mutex> lock(mtx);
    // std::ofstream f(log_, std::ios::app);
    // f.write((char *)&term, sizeof(int));
    // int size = cmd.size();
    // //audo c = std::vector<u8>(size);
    // auto c = cmd.serialize(size);
    // char *buf = new char[size];
    // memcpy(&buf[0], c.data(), size);
    // f.write((char *)&size, sizeof(int));
    // f.write(buf, size);
    // delete[] buf;
}

template <typename Command>
void RaftLog<Command>::writelogs(std::vector<std::pair<int, Command>> &vec) {
    std::unique_lock<std::mutex> lock(mtx);
    std::vector<u8> vector;
    // std::ofstream f(log_, std::ios::trunc);
    for (auto &ent : vec) {
        auto tmp = std::vector<u8>();
        auto term = ent.first;
        auto cmd = ent.second;
        // f.write((char *)&term, sizeof(int));
        tmp.insert(tmp.end(), (u8*)&term, (u8*)&term + 4);
        int size = cmd.size();
        auto ch = cmd.serialize(size);
        u8 *buf = new u8[size];
        memcpy(&buf[0], ch.data(), size);
        // f.write((char *)&size, sizeof(int));
        tmp.insert(tmp.end(), (u8*)&size, (u8*)&size + 4);
        // f.write(buf, size);
        tmp.insert(tmp.end(), buf, buf + size);
        vector.insert(vector.end(), tmp.begin(), tmp.end());
        delete[] buf;
    }
    int length = vector.size();
    log_bm->write_partial_block(0, (u8*)&length, 0, 4);
    auto left = length;
    auto i = length % DiskBlockSize == 0 ? length / DiskBlockSize : length / DiskBlockSize + 1;
    for (int st = 1; st <= i; st++) {
        auto tmp = std::vector<u8>(vector.begin() + length - left, vector.begin() + length - left + (left >= DiskBlockSize ? DiskBlockSize : left));
        log_bm->write_partial_block(st, tmp.data(), 0, tmp.size());
        left -= DiskBlockSize;
    }
    log_bm->flush();
}

template <typename Command>
void RaftLog<Command>::recover(std::vector<std::pair<int, Command>> &log, int &current_term, int &commit_index, int &last_applied, int &vote_for) {
    
    std::unique_lock<std::mutex> _(mtx);
    {
        auto buffer = std::vector<u8>();
        auto meta1 = std::vector<u8>(DiskBlockSize);
        log_bm->read_block(0, meta1.data());
        auto ptr = meta1.data();
        auto log_sz = ((int *)ptr)[0];
        auto left = log_sz;
        auto i = log_sz % DiskBlockSize == 0 ? log_sz / DiskBlockSize : log_sz / DiskBlockSize + 1;
        for (auto st = 1; st <= i; st++) {
            auto tmp = std::vector<u8>(DiskBlockSize);
            log_bm->read_block(st, tmp.data());
            buffer.insert(buffer.end(), tmp.begin(), tmp.begin() + (left >= DiskBlockSize ? DiskBlockSize : left));
            left = left > DiskBlockSize ? left - DiskBlockSize : 0;
        }
        auto cc = 0;
        ptr = buffer.data();
        while (cc < log_sz) {
            auto ptr1 = (int *)(&ptr[cc]);
            auto term = ptr1[0];
            cc += 4;
            auto ptr2 = (int *)(&ptr[cc]);
            auto size = ptr2[0];
            cc += 4;
            // auto ptr3 = &ptr[cc];
            auto comd = Command{};
            std::vector<u8> cmd = std::vector<u8>(buffer.begin() + cc, buffer.begin() + cc + size);
            cc += size;
            comd.deserialize(cmd, size);
            log.emplace_back(term, comd);
        }
        // std::ifstream f(log_);
        // if(f.fail()){
        //     return;
        // }
        // int term;
        // f.read((char*)&term,sizeof(int));
        // while(!f.eof()){
        //     auto cmd = Command{};
        //     int size;
        //     f.read((char*)&size,sizeof(int));
        //     char *c = new char[size];
        //     f.read(c,size);
        //     std::vector<u8> tmp(size);
        //     memcpy(tmp.data(), c, size);
        //     cmd.deserialize(tmp ,size);
        //     log.emplace_back(term,cmd);
        //     f.read((char*)&term,sizeof(int));
        //     delete[] c;
        // }
    }
    {
        auto tmp = std::vector<u8>(DiskBlockSize);
        meta_bm->read_block(0, tmp.data());
        auto ptr = tmp.data();
        current_term = ((int *)ptr)[0];
        commit_index = ((int *)ptr)[1];
        last_applied = ((int *)ptr)[2];
        vote_for = ((int *)ptr)[3];
    }
}

template <typename Command>
void RaftLog<Command>::updatemeta(int current_term, int commit_index, int last_applied, int vote_for){
    std::unique_lock<std::mutex> _(mtx);
    // std::ofstream f(meta_, std::ios::trunc);
    // f << current_term << " " << commit_index << " " << last_applied << " " << vote_for;
    meta_bm->write_partial_block(0, (u8*)&current_term, 0, 4);
    meta_bm->write_partial_block(0, (u8*)&commit_index, 4, 4);
    meta_bm->write_partial_block(0, (u8*)&last_applied, 8, 4);
    meta_bm->write_partial_block(0, (u8*)&vote_for, 12, 4);
    meta_bm->flush();
}

template <typename Command>
void RaftLog<Command>::save_snap(int last_snap_idx, int last_snap_term, std::vector<u8> &data){
    std::unique_lock<std::mutex> _(mtx);
    // std::ofstream f(snap_, std::ios::trunc);
    // int length = data.size();
    // f.write((char*)&last_snap_idx, sizeof(int));
    // f.write((char*)&last_snap_term, sizeof(int));
    // f.write((char*)&length, sizeof(int));
    // std::string data_;
    // char *buf = new char[data.size()];
    // memcpy(&buf[0], data.data(), data.size());
    // // data_.assign(data.begin(), data.end());
    // f.write(buf, data.size());
    // delete[] buf;
    int length = data.size();
    snap_bm->write_partial_block(0, (u8*)&last_snap_idx, 0, 4);
    snap_bm->write_partial_block(0, (u8*)&last_snap_term, 4, 4);
    snap_bm->write_partial_block(0, (u8*)&length, 8, 4);
    auto i = (length % DiskBlockSize == 0) ? length / DiskBlockSize : length /DiskBlockSize + 1;
    auto left_sz = length;
    for (auto st = 1; st <= i; st++) {
        auto tmp = std::vector(data.begin() + length - left_sz, data.begin() + length - left_sz + (left_sz >= DiskBlockSize ? DiskBlockSize : left_sz));
        snap_bm->write_partial_block(st, tmp.data(), 0, tmp.size());
        left_sz -= tmp.size();
    }
    snap_bm->flush();
}

template <typename Command>
void RaftLog<Command>::read_snap(int &last_snap_idx, int &last_snap_term, std::vector<u8> &data) {
    std::unique_lock<std::mutex> lock(mtx);
    {
        // std::ifstream f(snap_);
        // if(f.fail()){
        //     return;
        // }
        // int i;
        // int len;
        // f.read((char*)&i,sizeof(int));
        // last_snap_idx = i;
        // f.read((char*)&i,sizeof(int));
        // last_snap_term = i;
        // f.read((char*)&len, sizeof(int));
        // u8 *dt = new u8[len];
        // data.resize(len);
        // f.read((char*)dt, len);
        // memcpy(data.data(), dt, len);
        // delete[] dt;
        std::vector<u8> dt(DiskBlockSize);
        snap_bm->read_block(0, dt.data());
        auto ptr = dt.data();
        last_snap_idx = ((int *)ptr)[0];
        last_snap_term = ((int *)ptr)[1];
        auto length = ((int *)ptr)[2];
        auto i = length % DiskBlockSize == 0 ? length / DiskBlockSize : length / DiskBlockSize + 1;
        auto left_sz = length;
        for (auto st = 1; st <= i; st++) {
            auto tmp = std::vector<u8>(DiskBlockSize);
            snap_bm->read_block(st, tmp.data());
            data.insert(data.end(), tmp.begin(), tmp.begin() + (left_sz >= DiskBlockSize ? DiskBlockSize : left_sz));
            left_sz = left_sz > DiskBlockSize ? left_sz - DiskBlockSize : 0;
        }
    }
}

} /* namespace chfs */

#pragma once

#include <atomic>
#include <mutex>
#include <chrono>
#include <thread>
#include <ctime>
#include <algorithm>
#include <thread>
#include <memory>
#include <stdarg.h>
#include <unistd.h>
#include <filesystem>

#include "rsm/state_machine.h"
#include "rsm/raft/log.h"
#include "rsm/raft/protocol.h"
#include "utils/thread_pool.h"
#include "librpc/server.h"
#include "librpc/client.h"
#include "block/manager.h"

namespace chfs {

enum class RaftRole {
    Follower,
    Candidate,
    Leader
};

struct RaftNodeConfig {
    int node_id;
    uint16_t port;
    std::string ip_address;
};

// template <typename Command>
// class entries {
//     public:
//         int term;
//         Command cmd;
// };

template <typename StateMachine, typename Command>
class RaftNode {

#define RAFT_LOG(fmt, args...)                                                                                   \
    do {                                                                                                         \
        auto now =                                                                                               \
            std::chrono::duration_cast<std::chrono::milliseconds>(                                               \
                std::chrono::system_clock::now().time_since_epoch())                                             \
                .count();                                                                                        \
        char buf[512];                                                                                      \
        sprintf(buf,"[%ld][%s:%d][node %d term %d role %d] " fmt "\n", now, __FILE__, __LINE__, my_id, current_term, role, ##args); \
        thread_pool->enqueue([=]() { std::cerr << buf;} );                                         \
    } while (0);

public:
    RaftNode (int node_id, std::vector<RaftNodeConfig> node_configs);
    ~RaftNode();

    /* interfaces for test */
    void set_network(std::map<int, bool> &network_availablility);
    void set_reliable(bool flag);
    int get_list_state_log_num();
    int rpc_count();
    std::vector<u8> get_snapshot_direct();

private:
    /* 
     * Start the raft node.
     * Please make sure all of the rpc request handlers have been registered before this method.
     */
    auto start() -> int;

    /*
     * Stop the raft node.
     */
    auto stop() -> int;
    
    /* Returns whether this node is the leader, you should also return the current term. */
    auto is_leader() -> std::tuple<bool, int>;

    /* Checks whether the node is stopped */
    auto is_stopped() -> bool;

    /* 
     * Send a new command to the raft nodes.
     * The returned tuple of the method contains three values:
     * 1. bool:  True if this raft node is the leader that successfully appends the log,
     *      false If this node is not the leader.
     * 2. int: Current term.
     * 3. int: Log index.
     */
    auto new_command(std::vector<u8> cmd_data, int cmd_size) -> std::tuple<bool, int, int>;

    /* Save a snapshot of the state machine and compact the log. */
    auto save_snapshot() -> bool;

    /* Get a snapshot of the state machine */
    auto get_snapshot() -> std::vector<u8>;


    /* Internal RPC handlers */
    auto request_vote(RequestVoteArgs arg) -> RequestVoteReply;
    auto append_entries(RpcAppendEntriesArgs arg) -> AppendEntriesReply;
    auto install_snapshot(InstallSnapshotArgs arg) -> InstallSnapshotReply;

    /* RPC helpers */
    void send_request_vote(int target, RequestVoteArgs arg);
    void handle_request_vote_reply(int target, const RequestVoteArgs arg, const RequestVoteReply reply);

    void send_append_entries(int target, AppendEntriesArgs<Command> arg);
    void handle_append_entries_reply(int target, const AppendEntriesArgs<Command> arg, const AppendEntriesReply reply);

    void send_install_snapshot(int target, InstallSnapshotArgs arg);
    void handle_install_snapshot_reply(int target, const InstallSnapshotArgs arg, const InstallSnapshotReply reply);

    /* background workers */
    void run_background_ping();
    void run_background_election();
    void run_background_commit();
    void run_background_apply();

    auto get_time() -> unsigned long;
    auto get_random_timer() -> int;
    auto get_last_logical() -> std::pair<int, int>;


    /* Data structures */
    bool network_stat;          /* for test */

    std::mutex mtx;                             /* A big lock to protect the whole data structure. */
    std::mutex clients_mtx;                     /* A lock to protect RpcClient pointers */
    std::unique_ptr<ThreadPool> thread_pool;
    std::unique_ptr<RaftLog<Command>> log_storage;     /* To persist the raft log. */
    std::unique_ptr<StateMachine> state;  /*  The state machine that applies the raft log, e.g. a kv store. */

    std::unique_ptr<RpcServer> rpc_server;      /* RPC server to recieve and handle the RPC requests. */
    std::map<int, std::unique_ptr<RpcClient>> rpc_clients_map;  /* RPC clients of all raft nodes including this node. */
    std::vector<RaftNodeConfig> node_configs;   /* Configuration for all nodes */ 
    int my_id;                                  /* The index of this node in rpc_clients, start from 0. */

    std::atomic_bool stopped;

    RaftRole role;
    int current_term;
    int leader_id;

    std::unique_ptr<std::thread> background_election;
    std::unique_ptr<std::thread> background_ping;
    std::unique_ptr<std::thread> background_commit;
    std::unique_ptr<std::thread> background_apply;

    /* Lab3: Your code here */
    int votefor;
    std::vector<std::pair<int, Command>> log_entries;
    std::atomic_int voter_num;

    int commit_idx;
    int last_applied;
    unsigned long last_rpc;

    std::map<int, int> next_index;
    std::map<int, int> match_index;

    int last_snap_idx;
    int last_snap_term;
    std::vector<u8> data;

};

template <typename StateMachine, typename Command>
RaftNode<StateMachine, Command>::RaftNode(int node_id, std::vector<RaftNodeConfig> configs):
    network_stat(true),
    node_configs(configs),
    my_id(node_id),
    stopped(false),
    role(RaftRole::Follower),
    current_term(0),
    leader_id(-1),
    last_snap_idx(0),
    last_snap_term(0)
    //srand(time(NULL))
{
    srand(time(NULL));
    log_storage = std::make_unique<RaftLog<Command>>(nullptr, my_id);
    votefor = -1;
    // std::pair<int, Command> tmp;
    // tmp.first = 0;
    // log_entries.emplace_back(tmp);
    commit_idx = 0;
    last_applied = 0;
    last_rpc = get_time();
    thread_pool = std::make_unique<ThreadPool>(32);
    auto my_config = node_configs[my_id];
    state = std::make_unique<StateMachine>();
    
    log_storage->recover(log_entries, current_term, commit_idx, last_applied, votefor);
    log_storage->read_snap(last_snap_idx, last_snap_term, data);
    if (last_snap_idx) {
        state->apply_snapshot(data);
    }
    if (log_entries.empty()) {
        if (!last_snap_idx) {
            std::pair<int, Command> tmp;
            tmp.first = 0;
            log_entries.push_back(tmp);
            log_storage->writelogs(log_entries);
        }
    } else {
        auto iter = log_entries.begin();
        auto ed = log_entries.begin();
        if (!last_snap_idx) {
            iter++;
            std::advance(ed, last_applied + 1);
        } else {
            std::advance(ed, last_applied - last_snap_idx);
        }
        for (; iter != ed; iter++) {
            state->apply_log(iter->second);
        }
    }

    /* launch RPC server */
    rpc_server = std::make_unique<RpcServer>(my_config.ip_address, my_config.port);

    /* Register the RPCs. */
    rpc_server->bind(RAFT_RPC_START_NODE, [this]() { return this->start(); });
    rpc_server->bind(RAFT_RPC_STOP_NODE, [this]() { return this->stop(); });
    rpc_server->bind(RAFT_RPC_CHECK_LEADER, [this]() { return this->is_leader(); });
    rpc_server->bind(RAFT_RPC_IS_STOPPED, [this]() { return this->is_stopped(); });
    rpc_server->bind(RAFT_RPC_NEW_COMMEND, [this](std::vector<u8> data, int cmd_size) { return this->new_command(data, cmd_size); });
    rpc_server->bind(RAFT_RPC_SAVE_SNAPSHOT, [this]() { return this->save_snapshot(); });
    rpc_server->bind(RAFT_RPC_GET_SNAPSHOT, [this]() { return this->get_snapshot(); });

    rpc_server->bind(RAFT_RPC_REQUEST_VOTE, [this](RequestVoteArgs arg) { return this->request_vote(arg); });
    rpc_server->bind(RAFT_RPC_APPEND_ENTRY, [this](RpcAppendEntriesArgs arg) { return this->append_entries(arg); });
    rpc_server->bind(RAFT_RPC_INSTALL_SNAPSHOT, [this](InstallSnapshotArgs arg) { return this->install_snapshot(arg); });

   /* Lab3: Your code here */ 

    rpc_server->run(true, configs.size()); 
    
}

template <typename StateMachine, typename Command>
RaftNode<StateMachine, Command>::~RaftNode()
{
    stop();

    thread_pool.reset();
    rpc_server.reset();
    state.reset();
    log_storage.reset();
}

/******************************************************************

                        RPC Interfaces

*******************************************************************/


template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::start() -> int
{
    /* Lab3: Your code here */
    RAFT_LOG("start");
    // auto my_config = node_configs[my_id];
    for (auto config : node_configs) {
        RAFT_LOG("init client: %d", config.node_id);
        rpc_clients_map[config.node_id] = std::make_unique<RpcClient>(config.ip_address, config.port, true);
    }
    background_election = std::make_unique<std::thread>(&RaftNode::run_background_election, this);
    background_ping = std::make_unique<std::thread>(&RaftNode::run_background_ping, this);
    background_commit = std::make_unique<std::thread>(&RaftNode::run_background_commit, this);
    background_apply = std::make_unique<std::thread>(&RaftNode::run_background_apply, this);
    std::cout << "start ed" << std::endl;
    return 0;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::stop() -> int
{
    /* Lab3: Your code here */
    stopped.store(true);
    background_apply->join();
    background_commit->join();
    background_ping->join();
    background_election->join();
    thread_pool.reset();
    return 0;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::is_leader() -> std::tuple<bool, int>
{
    /* Lab3: Your code here */
    return std::make_tuple(role == RaftRole::Leader, current_term);
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::is_stopped() -> bool
{
    return stopped.load();
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::new_command(std::vector<u8> cmd_data, int cmd_size) -> std::tuple<bool, int, int>
{
    /* Lab3: Your code here */
    Command ccc;
    ccc.deserialize(cmd_data, cmd_data.size());
    RAFT_LOG("receive %d", ccc.value);
    std::unique_lock<std::mutex> lock(mtx);
    Command command;
    if(role == RaftRole::Leader) {
        std::pair<int, Command> log_entry;
        log_entry.first = current_term;
        command.deserialize(cmd_data, cmd_data.size());
        log_entry.second = command;
        // RAFT_LOG("COMMAND VALUE %d\n", command.value);
        log_entries.emplace_back(log_entry);
        auto index = get_last_logical().first;
        //todo do
        log_storage->writelogs(log_entries);
        return std::make_tuple(true, current_term, index);
    } 
    return std::make_tuple(false, -1, -1);
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::save_snapshot() -> bool
{
    /* Lab3: Your code here */ 
    std::unique_lock<std::mutex> lock(mtx);
    data = state->snapshot();
    // auto log_it = log_entries.begin();
    // std::advance(log_it,last_applied - last_snap_idx);
    // bool empty = log_it == log_entries.begin();
    // if(last_snap_idx && !empty){
    //     log_it--;
    // }
    // last_snap_idx = last_applied;
    // last_snap_term = empty ? last_snap_term : log_it->first;
    // log_it++;
    // log_entries.erase(log_entries.begin(),log_it);

    auto idx = last_snap_idx == 0 ? last_applied : last_applied - last_snap_idx - 1;
    RAFT_LOG("last applied: %d, last snap %d", last_applied, last_snap_idx);
    auto iter = log_entries.begin();
    for (auto i = 0; i <= idx; i++) {
        iter++;
    }
    if (iter == log_entries.begin())
        return true;
    else {
        last_snap_idx = last_applied;
        last_snap_term = log_entries[idx].first;
        log_entries.erase(log_entries.begin(), iter);
        state->apply_snapshot(data);
        log_storage->save_snap(last_snap_idx, last_snap_term, data);
        log_storage->writelogs(log_entries);
        // return true;
    }
    RAFT_LOG("snap size: %d", (int)state->snapshot().size());

    if(role != RaftRole::Leader){
        return true;
    }
    
    InstallSnapshotArgs arg;
    arg.term = current_term;
    arg.leader_id = my_id;
    arg.last_snap_idx = last_snap_idx;
    arg.last_snap_term = last_snap_term;
    arg.data = data;
    for(auto &cli : rpc_clients_map){
        thread_pool->enqueue(&RaftNode::send_install_snapshot, this, cli.first, arg);
    }


    return true;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::get_snapshot() -> std::vector<u8>
{
    /* Lab3: Your code here */
    std::unique_lock<std::mutex> lock(mtx);
    return state->snapshot();
    // return std::vector<u8>();
}

/******************************************************************

                         Internal RPC Related

*******************************************************************/


template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::request_vote(RequestVoteArgs args) -> RequestVoteReply
{
    std::unique_lock<std::mutex> lock(mtx);
    RequestVoteReply requvostreply;
    requvostreply.CurrentTerm = current_term;
    // auto entry_size = log_entries.size();
    auto p = get_last_logical();
    auto last_idx = p.first;
    auto last_term = p.second;
    last_rpc = get_time();

    if (votefor >= 0 && current_term == args.TermID) {
        requvostreply.IsVoted = false;
        return requvostreply;
    }
    if (current_term < args.TermID) {
        role = RaftRole::Follower;
        votefor = -1;
        current_term = args.TermID;
    } 
    // auto s = log_entries[entry_size - 1];
    if (args.TermID < current_term || last_term > args.LastLogTerm || (last_term == args.LastLogTerm && last_idx > args.LastLogIndex)) {
        // RAFT_LOG("arg.term: %d, log.term: %d, argslastterm: %d, arg.lastidx: %d\n", args.TermID, log_entries[entry_size - 1].first, args.LastLogTerm, args.LastLogIndex);
        requvostreply.IsVoted = false;
        // RAFT_LOG("not vote for %d for old", args.TermID);
        return requvostreply;
    }
    requvostreply.IsVoted = true;
    role = RaftRole::Follower;
    votefor = args.CandidateID;
    current_term = args.TermID;
    log_storage->updatemeta(current_term, commit_idx, last_applied, votefor);
    RAFT_LOG("change term in request %d, votefor %d", current_term, votefor);

    // todo
    return requvostreply;

    /* Lab3: Your code here */
    // return RequestVoteReply();
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::handle_request_vote_reply(int target, const RequestVoteArgs arg, const RequestVoteReply reply)
{
    /* Lab3: Your code here */
    std::unique_lock<std::mutex> lock(mtx);
    // RAFT_LOG("Reveice Reply %d", my_id);
    if (reply.CurrentTerm > current_term) {
        current_term = reply.CurrentTerm;
        RAFT_LOG("change term in handle request %d", current_term);
        role = RaftRole::Follower;
        votefor = -1;
        log_storage->updatemeta(current_term, commit_idx, last_applied, votefor);
        return;
    } else if (reply.IsVoted == true && role == RaftRole::Candidate) {
        voter_num++;
        if (voter_num >= rpc_clients_map.size() / 2 + 1) {
            role = RaftRole::Leader;
            auto p = get_last_logical();
            for (auto i = 0; i < rpc_clients_map.size(); i++) {
                next_index.emplace(i, p.first + 1);
                match_index.emplace(i, 0);
            }
        }
    }
    return;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::append_entries(RpcAppendEntriesArgs rpc_arg) -> AppendEntriesReply
{
    /* Lab3: Your code here */
    std::unique_lock<std::mutex> lck(mtx);
    AppendEntriesReply reply;
    
    AppendEntriesArgs<Command> true_args = transform_rpc_append_entries_args<Command>(rpc_arg);
    // if (rpc_arg.log.size() > 0)
    //     RAFT_LOG("true args: %d\n", (int)true_args.log.size());
    auto p = get_last_logical();
    if (true_args.log.size() == 0 && true_args.prev_log_index == -1 && true_args.prev_log_term == -1) {
        if (true_args.term >= current_term) {
            if (true_args.term == current_term && true_args.leader_commit > commit_idx && true_args.leader_commit < p.first + 1) {
                commit_idx = true_args.leader_commit < p.first ? true_args.leader_commit : p.first;
                // todo
            }
            current_term = true_args.term;
            role = RaftRole::Follower;
            last_rpc = get_time();
            // todo
            reply.term = true_args.term;
            reply.success = true;
            log_storage->updatemeta(current_term, commit_idx, last_applied, votefor);
            return reply;
        } else {
            last_rpc = get_time();
            reply.term = current_term;
            reply.success = false;
            return reply;
        }
    } else {
        RAFT_LOG("receive appends\n")
        if (true_args.term < current_term) {
            last_rpc = get_time();
            reply.term = current_term;
            reply.success = false;
            return reply;
        }
        current_term = true_args.term;
        role = RaftRole::Follower;
        auto p = get_last_logical();
        auto last_idx = last_snap_idx > 0 ? true_args.prev_log_index - last_snap_idx - 1 : true_args.prev_log_index;
        // todo
        log_storage->updatemeta(current_term, commit_idx, last_applied, votefor);
        RAFT_LOG("argprevidx: %d, logentsize: %d, prevterm: %d, lastidx %d, lastsnap %d", true_args.prev_log_index, (int)p.first, true_args.prev_log_term, last_idx, last_snap_idx);
        // auto term = last_idx >= 0 ? log_entries[last_idx].first : last_snap_term;
        if (true_args.prev_log_index <= p.first && true_args.prev_log_term == (last_idx >= 0 ? log_entries[last_idx].first : last_snap_term)) {
            log_entries.resize(last_idx + true_args.log.size() + 1);
            for (int i = last_idx + 1; i <= last_idx + true_args.log.size(); i++) {
                if (i < log_entries.size())
                    log_entries[i] = true_args.log[i - last_idx - 1];
            }

            if (true_args.leader_commit > commit_idx && true_args.leader_commit < p.first + 1) {
                commit_idx = true_args.leader_commit < p.first ? true_args.leader_commit : p.first;
            }
            last_rpc = get_time();
            // todo
            log_storage->updatemeta(current_term, commit_idx, last_applied, votefor);
            log_storage->writelogs(log_entries);
            reply.term = current_term;
            reply.success = true;
            return reply;
        } else {
            last_rpc = get_time();
            reply.term = current_term;
            reply.success = false;
        }
    }
    RAFT_LOG("not enter reply term %d", reply.term);
    return reply;
   
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::handle_append_entries_reply(int node_id, const AppendEntriesArgs<Command> arg, const AppendEntriesReply reply)
{
    /* Lab3: Your code here */
    std::unique_lock<std::mutex> lock(mtx);
    last_rpc = get_time();
    if (role != RaftRole::Leader)
        return;
    if (current_term < reply.term) {
        role = RaftRole::Follower;
        votefor = -1;
        current_term = reply.term;
        log_storage->updatemeta(current_term, commit_idx, last_applied, votefor);
        RAFT_LOG("change term in handdle append %d", current_term);
        // todo
        return;
    }
    if (arg.log.size() == 0 && arg.prev_log_index == -1 && arg.prev_log_term == -1) {
        // RAFT_LOG("reveive heart \n");
        return;
    } 
    //todo   
    if (reply.success == true) {
        int new_index = arg.prev_log_index + arg.log.size();
        match_index[node_id] = new_index;
        next_index[node_id] = new_index + 1;
        auto idx = last_snap_idx > 0 ? commit_idx - last_snap_idx - 1 : commit_idx;
        for (auto i = idx + 1; i < log_entries.size(); i++) {
            int re_num = 0;
            if (log_entries[i].first != current_term)
                continue;
            for (auto &cli : rpc_clients_map) {
                if (cli.first == my_id)
                    re_num++;
                else if (match_index[cli.first] >= (last_snap_idx == 0 ? i : i + last_snap_idx + 1)) {
                    re_num++;
                }
            }
            if (re_num >= (rpc_clients_map.size() + 1) / 2){
                    commit_idx = (last_snap_idx == 0 ? i : i + last_snap_idx + 1);
            } else 
                break;
            //todo
        }
    } else {
        if(arg.prev_log_index <= last_snap_idx && last_snap_idx > 0) {
            InstallSnapshotArgs args;
            args.term = current_term;
            args.leader_id = my_id;
            args.last_snap_idx = last_snap_idx;
            args.last_snap_term = last_snap_term;
            std::vector<u8> dt(data.begin(), data.end());
            args.data = dt;
            thread_pool->enqueue(&RaftNode::send_install_snapshot, this, node_id, args);
        } else {
            next_index[node_id] = (next_index[node_id] > 1) ? next_index[node_id] - 1 : 1;
        }
        // RAFT_LOG("prev %d is %d\n", node_id, next_index[node_id]);
        // RAFT_LOG("next[%d] changed to %d\n", node_id, next_index[node_id]);
    }
    RAFT_LOG("handled %d", node_id); 
    return;
}


template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::install_snapshot(InstallSnapshotArgs args) -> InstallSnapshotReply
{
    /* Lab3: Your code here */
    std::unique_lock<std::mutex> lock(mtx);
    RAFT_LOG("READY INSTALL %d", args.last_snap_idx);
    InstallSnapshotReply reply;
    reply.term = current_term;
    if (args.term >= current_term) {
        current_term = args.term;
        reply.term = current_term;
        auto idx = get_last_logical().first;
        if (idx < args.last_snap_idx) {
            log_entries.clear();
        } else {
            auto iter = log_entries.begin();
            std::advance(iter, args.last_snap_idx - last_snap_idx);
            if (!last_snap_idx) {
                iter++;
            }
            log_entries.erase(log_entries.begin(), iter);
        }
        auto data_ = args.data;
        data.swap(data_);
        RAFT_LOG("data size: %d", (int)data.size());
        last_snap_idx = args.last_snap_idx;
        last_snap_term = args.last_snap_term;
        last_applied = last_snap_idx;
        state->apply_snapshot(data);
        RAFT_LOG("snap sz: %d", (int)state->snapshot().size());
        log_storage->save_snap(last_snap_idx, last_snap_term, data);
        log_storage->updatemeta(current_term, commit_idx, last_applied, votefor);
        log_storage->writelogs(log_entries);
    } else {

    }
    return reply;
}


template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::handle_install_snapshot_reply(int node_id, const InstallSnapshotArgs arg, const InstallSnapshotReply reply)
{
    /* Lab3: Your code here */
    RAFT_LOG("Reply term %d, arg.term %d", reply.term, arg.term);
    if (reply.term > arg.term) {
        current_term = current_term > arg.term ? current_term : reply.term;
        role = RaftRole::Follower;
        log_storage->updatemeta(current_term, commit_idx, last_applied);
    } else {
        next_index[node_id] = arg.last_snap_idx + 1;
        match_index[node_id] = arg.last_snap_idx;
    }
    return;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::send_request_vote(int target_id, RequestVoteArgs arg)
{
    std::unique_lock<std::mutex> clients_lock(clients_mtx);
    if (rpc_clients_map[target_id] == nullptr
        || rpc_clients_map[target_id]->get_connection_state() != rpc::client::connection_state::connected) {
        // RAFT_LOG("returned here \n");
        return;
    }

    auto res = rpc_clients_map[target_id]->call(RAFT_RPC_REQUEST_VOTE, arg);
    clients_lock.unlock();
    if (res.is_ok()) {
        handle_request_vote_reply(target_id, arg, res.unwrap()->as<RequestVoteReply>());
    } else {
        // RPC fails
    }
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::send_append_entries(int target_id, AppendEntriesArgs<Command> arg)
{
    std::unique_lock<std::mutex> clients_lock(clients_mtx);
    if (rpc_clients_map[target_id] == nullptr 
        || rpc_clients_map[target_id]->get_connection_state() != rpc::client::connection_state::connected) {
        // RAFT_LOG("returned here @ %d", target_id);
        return;
    }

    RpcAppendEntriesArgs rpc_arg = transform_append_entries_args(arg);
    // RAFT_LOG("rpcarglog size is : %d\n", (int)arg.log.size());
    auto res = rpc_clients_map[target_id]->call(RAFT_RPC_APPEND_ENTRY, rpc_arg);
    if (arg.log.size() != 0) {
        RAFT_LOG("SEND SUCCESS %d", target_id);
    }
    clients_lock.unlock();
    if (res.is_ok()) {
        handle_append_entries_reply(target_id, arg, res.unwrap()->as<AppendEntriesReply>());
    } else {
        // RPC fails
        RAFT_LOG("failed ???\n");
    }
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::send_install_snapshot(int target_id, InstallSnapshotArgs arg)
{
    std::unique_lock<std::mutex> clients_lock(clients_mtx);
    if (rpc_clients_map[target_id] == nullptr
        || rpc_clients_map[target_id]->get_connection_state() != rpc::client::connection_state::connected) {
        return;
    }

    auto res = rpc_clients_map[target_id]->call(RAFT_RPC_INSTALL_SNAPSHOT, arg);
    clients_lock.unlock();
    if (res.is_ok()) { 
        handle_install_snapshot_reply(target_id, arg, res.unwrap()->as<InstallSnapshotReply>());
    } else {
        // RPC fails
    }
}


/******************************************************************

                        Background Workers

*******************************************************************/

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_election() {
    // Periodly check the liveness of the leader.

    // Work for followers and candidates.

    /* Uncomment following code when you finish */
    auto ele_time = 0;
    while (true) {
        // RAFT_LOG("here  here\n");
        if (is_stopped()) {
                RAFT_LOG("returned \n");
                return;
            }
            /* Lab3: Your code here */
            // std::unique_lock<std::mutex> lock(mtx);
        mtx.lock();
        // unsigned long random_t = (role == RaftRole::Follower ? 100 : 500) + (rand() % 200);
        // RAFT_LOG("here \n");
        if (role != RaftRole::Leader && get_time() - last_rpc > get_random_timer()) {
            // RAFT_LOG("into \n");
            role = RaftRole::Candidate;
            ele_time++;
            current_term++;
            last_rpc = get_time();
            votefor = my_id;
            voter_num = 1;
            RequestVoteArgs args;
            args.TermID = current_term;
            args.CandidateID = my_id;
            auto p = get_last_logical();
            args.LastLogIndex = p.first;
            args.LastLogTerm = p.second;
            RAFT_LOG("logsize: %d, lastterm: %d", (int)log_entries.size(), args.LastLogTerm);
            for (auto &client : rpc_clients_map) {
                if (client.first == my_id)
                    continue;
                thread_pool->enqueue(&RaftNode::send_request_vote, this, client.first, args);
            }
            if (ele_time == 3) {
                //std::this_thread::sleep_for(std::chrono::milliseconds(300));
            }
        }
        mtx.unlock();    
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    return;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_commit() {
    // Periodly send logs to the follower.

    // Only work for the leader.

    /* Uncomment following code when you finish */
    while (true) {
        if (is_stopped()) {
            return;
        }
        /* Lab3: Your code here */
        mtx.lock();
        if (role == RaftRole::Leader) {
            for (auto &cli : rpc_clients_map) {
                if (cli.first == my_id)
                    continue;
                auto last_idx = last_snap_idx == 0 ? log_entries.size() - 1 : log_entries.size() + last_snap_idx;
                RAFT_LOG("nextidx %d, last snap %d, cliidx %d, logsize: %d", next_index[cli.first], last_snap_idx, cli.first, (int)last_idx);
                if (next_index[cli.first] <= last_idx && next_index[cli.first] > last_snap_idx ) {
                    RAFT_LOG("enter here");
                    AppendEntriesArgs<Command> arg;
                    arg.term = current_term;
                    arg.leader_id = my_id;
                    arg.prev_log_index = next_index[cli.first] - 1;
                    auto term = last_snap_idx ? next_index[cli.first] - 1 - last_snap_idx - 1 : next_index[cli.first] - 1;
                    arg.prev_log_term = term >= 0 ? log_entries[term].first : last_snap_term;
                    arg.leader_commit = commit_idx;
                    // RAFT_LOG("%d, %d, %d", arg.prev_log_index, arg.prev_log_term, arg.leader_commit);
                    for (auto i = 0; i < log_entries.size() - term - 1; i++) {
                        arg.log.emplace_back(log_entries[term + 1 + i]);
                    }
                    // RAFT_LOG("size is %d\n", (int)arg.log.size());
                    thread_pool->enqueue(&RaftNode::send_append_entries, this, cli.first, arg);
                    // RAFT_LOG("send to %d\n", cli.first);
                } else if ((next_index[cli.first] <= last_idx && next_index[cli.first] <= last_snap_idx) || (match_index[cli.first] <= last_snap_idx && last_snap_idx > 0)) {
                    InstallSnapshotArgs arg;
                    arg.term = current_term;
                    arg.leader_id = my_id;
                    arg.last_snap_idx = last_snap_idx;
                    arg.last_snap_term = last_snap_term;
                    arg.data = data;
                    thread_pool->enqueue(&RaftNode::send_install_snapshot, this, cli.first, arg);
                    RAFT_LOG("send here");
                }
            }
        }
        mtx.unlock();
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }

    return;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_apply() {
    // Periodly apply committed logs the state machine

    // Work for all the nodes.

    /* Uncomment following code when you finish */
    while (true) {
        if (is_stopped()) {
            return;
        }
        mtx.lock();
        
        if (last_applied < commit_idx) {
            RAFT_LOG("LAST: %d; COM_IDX: %d\n", last_applied, commit_idx);
            auto begin = last_snap_idx == 0 ? last_applied + 1 : last_applied - last_snap_idx;
            auto end = last_snap_idx == 0 ? commit_idx : commit_idx - last_snap_idx - 1;
            for (auto i = begin; i <= end; i++) {
                state->apply_log(log_entries[i].second);
            }
            last_applied = commit_idx;
            //todo 
            log_storage->updatemeta(current_term, commit_idx, last_applied, votefor);
        }
        mtx.unlock();
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
            /* Lab3: Your code here */
    }

    return;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_ping() {
    // Periodly send empty append_entries RPC to the followers.

    // Only work for the leader.

    /* Uncomment following code when you finish */
    while (true) {
        if (is_stopped()) {
            return;
        }
        mtx.lock();
        if (role == RaftRole::Leader) {
            for (auto &client : rpc_clients_map) {
                if (client.first == my_id)
                    continue;
                AppendEntriesArgs<Command> rpc;
                rpc.term = current_term;
                // RAFT_LOG("true term %d", current_term);
                // RAFT_LOG("current term %d", rpc.term);
                rpc.leader_id = leader_id;
                // rpc.log = std::list<std::pair<int, Command>>>();
                rpc.leader_commit = commit_idx;
                rpc.prev_log_index = -1;
                //auto term = last_snap_idx ? next_index[client.first] - 1 - last_snap_idx - 1 : next_index[client.first] - 1;
                //RAFT_LOG("next is %d, size is %d, term idx is %d", next_index[client.first], (int)log_entries.size(), term);
                rpc.prev_log_term = -1;
                rpc.isheart = true;
                thread_pool->enqueue(&RaftNode::send_append_entries, this, client.first, rpc);
                // send_append_entries(client.first, rpc);
                // RAFT_LOG("send heart %d, send to %d", leader_id, client.first);
            }
        }
        mtx.unlock();
        std::this_thread::sleep_for(std::chrono::milliseconds(75));
    }

    return;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::get_time() -> unsigned long {
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}

/******************************************************************

                          Test Functions (must not edit)

*******************************************************************/

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::set_network(std::map<int, bool> &network_availability)
{
    std::unique_lock<std::mutex> clients_lock(clients_mtx);

    /* turn off network */
    if (!network_availability[my_id]) {
        for (auto &&client: rpc_clients_map) {
            if (client.second != nullptr)
                client.second.reset();
        }

        return;
    }

    for (auto node_network: network_availability) {
        int node_id = node_network.first;
        bool node_status = node_network.second;

        if (node_status && rpc_clients_map[node_id] == nullptr) {
            RaftNodeConfig target_config;
            for (auto config: node_configs) {
                if (config.node_id == node_id) 
                    target_config = config;
            }

            rpc_clients_map[node_id] = std::make_unique<RpcClient>(target_config.ip_address, target_config.port, true);
        }

        if (!node_status && rpc_clients_map[node_id] != nullptr) {
            rpc_clients_map[node_id].reset();
        }
    }
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::set_reliable(bool flag)
{
    std::unique_lock<std::mutex> clients_lock(clients_mtx);
    for (auto &&client: rpc_clients_map) {
        if (client.second) {
            client.second->set_reliable(flag);
        }
    }
}

template <typename StateMachine, typename Command>
int RaftNode<StateMachine, Command>::get_list_state_log_num()
{
    /* only applied to ListStateMachine*/
    std::unique_lock<std::mutex> lock(mtx);

    return state->num_append_logs;
}

template <typename StateMachine, typename Command>
int RaftNode<StateMachine, Command>::rpc_count()
{
    int sum = 0;
    std::unique_lock<std::mutex> clients_lock(clients_mtx);

    for (auto &&client: rpc_clients_map) {
        if (client.second) {
            sum += client.second->count();
        }
    }
    
    return sum;
}

template <typename StateMachine, typename Command>
std::vector<u8> RaftNode<StateMachine, Command>::get_snapshot_direct()
{
    if (is_stopped()) {
        return std::vector<u8>();
    }

    std::unique_lock<std::mutex> lock(mtx);
    // RAFT_LOG("return size: %d", (int)state->snapshot().size());
    return state->snapshot(); 
}

template<typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::get_random_timer() -> int {
    //static int cnt = 0;
    if (role == RaftRole::Follower)
        return 100 + rand() % 200;
    else 
        return 500 + rand() % 200;
}

template<typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::get_last_logical() -> std::pair<int, int> {
    if (last_snap_idx) {
        auto last_logical_index = log_entries.size() + last_snap_idx;
        auto last_logical_term = log_entries.empty() ? last_snap_term : log_entries.back().first;
        return {last_logical_index,last_logical_term};
    } else {
        auto last_logical_index = log_entries.size() - 1;
        auto last_logical_term = log_entries.back().first;
        return {last_logical_index,last_logical_term};
    }
}

}
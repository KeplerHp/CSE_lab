#pragma once

#include "rsm/raft/log.h"
#include "rpc/msgpack.hpp"

namespace chfs {

const std::string RAFT_RPC_START_NODE = "start node";
const std::string RAFT_RPC_STOP_NODE = "stop node";
const std::string RAFT_RPC_NEW_COMMEND = "new commend";
const std::string RAFT_RPC_CHECK_LEADER = "check leader";
const std::string RAFT_RPC_IS_STOPPED = "check stopped";
const std::string RAFT_RPC_SAVE_SNAPSHOT = "save snapshot";
const std::string RAFT_RPC_GET_SNAPSHOT = "get snapshot";

const std::string RAFT_RPC_REQUEST_VOTE = "request vote";
const std::string RAFT_RPC_APPEND_ENTRY = "append entries";
const std::string RAFT_RPC_INSTALL_SNAPSHOT = "install snapshot";

struct RequestVoteArgs {
    /* Lab3: Your code here */
    int TermID;
    int CandidateID;
    int LastLogIndex;
    int LastLogTerm;
    MSGPACK_DEFINE(
        TermID,
        CandidateID,
        LastLogIndex,
        LastLogTerm
    )
};

struct RequestVoteReply {
    /* Lab3: Your code here */
    int CurrentTerm;
    bool IsVoted;
    MSGPACK_DEFINE(
        CurrentTerm,
        IsVoted
    )
};

template <typename Command>
struct AppendEntriesArgs {
    /* Lab3: Your code here */
    int term;
    int leader_id;
    int prev_log_index;
    int prev_log_term;
    std::vector<std::pair<int, Command>> log;
    int leader_commit;
    bool isheart = false;
};

struct RpcAppendEntriesArgs {
    /* Lab3: Your code here */
    int term;
    int leader_id;
    int prev_log_index;
    int prev_log_term;
    std::vector<std::pair<int, std::vector<u8>>> log;
    int leader_commit;
    bool isheart = false;
    MSGPACK_DEFINE(
        term,
        leader_id,
        prev_log_index,
        prev_log_term,
        log,
        leader_commit,
        isheart
    )
};

template <typename Command>
RpcAppendEntriesArgs transform_append_entries_args(const AppendEntriesArgs<Command> &arg)
{
    /* Lab3: Your code here */
    // RAFT_LOG("recv log size: %d\n", (int)arg.log.size());
    
    RpcAppendEntriesArgs rpc_args;
    rpc_args.term = arg.term;
    rpc_args.leader_id = arg.leader_id;
    rpc_args.prev_log_index = arg.prev_log_index;
    rpc_args.prev_log_term = arg.prev_log_term;
    std::vector<std::pair<int, std::vector<u8>>> logs;
    for (auto idx : arg.log) {
        std::vector<u8> s = idx.second.serialize(idx.second.size());
        logs.push_back(std::pair<int, std::vector<u8>>(idx.first, s));
    }
    rpc_args.log = logs;
    rpc_args.leader_commit = arg.leader_commit;
    // if(!arg.isheart) {
    //     std::cout << arg.log.size() << "   " << rpc_args.log.size() << std::endl;
    // }
    return rpc_args;
}

template <typename Command>
AppendEntriesArgs<Command> transform_rpc_append_entries_args(const RpcAppendEntriesArgs &rpc_arg)
{
    /* Lab3: Your code here */
    AppendEntriesArgs<Command> app;
    app.term = rpc_arg.term;
    app.leader_id = rpc_arg.leader_id;
    app.prev_log_index = rpc_arg.prev_log_index;
    app.prev_log_term = rpc_arg.prev_log_term;
    std::vector<std::pair<int, Command>> lgs;
    for (auto i : rpc_arg.log) {
        Command a;
        a.deserialize(i.second, i.second.size());
        lgs.push_back(std::pair<int, Command>(i.first, a));
    }
    app.log = lgs;
    app.leader_commit = rpc_arg.leader_commit;
    return app;
}

struct AppendEntriesReply {
    /* Lab3: Your code here */
    int term;
    bool success;
    MSGPACK_DEFINE(
        term,
        success
    )
};

struct InstallSnapshotArgs {
    /* Lab3: Your code here */
    int term;
    int leader_id;
    int last_snap_idx;
    int last_snap_term;
    std::vector<u8> data;
    MSGPACK_DEFINE(
        term,
        leader_id,
        last_snap_idx,
        last_snap_term,
        data
    )
};

struct InstallSnapshotReply {
    /* Lab3: Your code here */
    int term;
    MSGPACK_DEFINE(
        term
    )
};

} /* namespace chfs */
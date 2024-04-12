#pragma once

#include "rsm/raft/log.h"
#include "rpc/msgpack.hpp"

namespace chfs
{
    const node_id_t INVALID_NODE_ID = -1;

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

    struct RequestVoteArgs
    {
        /* Lab3: Your code here */
        commit_id_t last_log_idx_;
        term_id_t last_log_term_;
        term_id_t term_id_;
        node_id_t node_id_;
        MSGPACK_DEFINE(last_log_idx_, last_log_term_, term_id_, node_id_)
    };

    struct RequestVoteReply
    {
        /* Lab3: Your code here */
        bool is_vote_;
        term_id_t term_id_;

        MSGPACK_DEFINE(is_vote_, term_id_)
    };

    template <typename Command>
    struct AppendEntriesArgs
    {
        /* Lab3: Your code here */
        term_id_t term_;            /* Leader's term */
        node_id_t leader_id_;       /* for followers to redirect clients */
        commit_id_t leader_commit_; /* Leader's commit index */
        term_id_t prev_log_term_;
        commit_id_t prev_log_idx_;
        std::vector<LogEntry<Command>> entries_;
        bool heartbeat_;
        int last_included_idx;
    };

    struct RpcAppendEntriesArgs
    {
        /* Lab3: Your code here */

        MSGPACK_DEFINE(

        )
    };

    template <typename Command>
    RpcAppendEntriesArgs transform_append_entries_args(const AppendEntriesArgs<Command> &arg)
    {
        /* Lab3: Your code here */
        return RpcAppendEntriesArgs();
    }

    template <typename Command>
    AppendEntriesArgs<Command> transform_rpc_append_entries_args(const RpcAppendEntriesArgs &rpc_arg)
    {
        /* Lab3: Your code here */
        return AppendEntriesArgs<Command>();
    }

    struct AppendEntriesReply
    {
        /* Lab3: Your code here */

        bool accept_;
        term_id_t term_;

        MSGPACK_DEFINE(accept_, term_)
    };

    struct InstallSnapshotArgs
    {
        /* Lab3: Your code here */

        MSGPACK_DEFINE(

        )
    };

    struct InstallSnapshotReply
    {
        /* Lab3: Your code here */

        MSGPACK_DEFINE(

        )
    };

} /* namespace chfs */
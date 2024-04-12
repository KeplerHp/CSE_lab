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

namespace chfs
{

    enum class RaftRole
    {
        Follower,
        Candidate,
        Leader
    };

    struct RaftNodeConfig
    {
        int node_id;
        uint16_t port;
        std::string ip_address;
    };

    template <typename StateMachine, typename Command>
    class RaftNode
    {

#define RAFT_LOG(fmt, args...)                                                                                                       \
    do                                                                                                                               \
    {                                                                                                                                \
        auto now =                                                                                                                   \
            std::chrono::duration_cast<std::chrono::milliseconds>(                                                                   \
                std::chrono::system_clock::now().time_since_epoch())                                                                 \
                .count();                                                                                                            \
        char buf[512];                                                                                                               \
        sprintf(buf, "[%ld][%s:%d][node %d term %d role %d] " fmt "\n", now, __FILE__, __LINE__, my_id, current_term, role, ##args); \
        thread_pool->enqueue([=]() { std::cerr << buf; });                                                                           \
    } while (0);

    public:
        RaftNode(int node_id, std::vector<RaftNodeConfig> node_configs);
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

        /*  tools */
        RequestVoteArgs create_request_vote();

        void become_follower(term_id_t term, node_id_t leader);

        void become_candidate();

        void become_leader();

        bool is_more_up_to_data(term_id_t termId, int index);

        void recover_from_disk();

        void recover_state_from_disk();

        int get_last_include_idx() const
        {
            return log_storage->get_last_include_idx();
        }

        /* background workers */
        void run_background_ping();
        void run_background_election();
        void run_background_commit();
        void run_background_apply();

        /* Data structures */
        bool network_stat; /* for test */

        std::mutex mtx;         /* A big lock to protect the whole data structure. */
        std::mutex clients_mtx; /* A lock to protect RpcClient pointers */
        std::unique_ptr<ThreadPool> thread_pool;
        std::unique_ptr<RaftLog<Command>> log_storage; /* To persist the raft log. */
        std::unique_ptr<StateMachine> state;           /*  The state machine that applies the raft log, e.g. a kv store. */

        std::unique_ptr<RpcServer> rpc_server;                     /* RPC server to recieve and handle the RPC requests. */
        std::map<int, std::unique_ptr<RpcClient>> rpc_clients_map; /* RPC clients of all raft nodes including this node. */
        std::vector<RaftNodeConfig> node_configs;                  /* Configuration for all nodes */
        int my_id;                                                 /* The index of this node in rpc_clients, start from 0. */

        std::atomic_bool stopped;

        RaftRole role;
        int current_term;
        int leader_id;
        commit_id_t commit_idx = 0;
        int last_applied_idx = 1;
        node_id_t vote_for_;
        node_id_t vote_gain_;
        std::map<node_id_t, commit_id_t> next_index_map_;
        IntervalTimer rand_timer{};
        IntervalTimer fixed_timer{100};
        std::vector<u8> installing_data;
        int apply_cnt = 0;

        std::unique_ptr<std::thread> background_election;
        std::unique_ptr<std::thread> background_ping;
        std::unique_ptr<std::thread> background_commit;
        std::unique_ptr<std::thread> background_apply;

        /* Lab3: Your code here */
    };

    template <typename StateMachine, typename Command>
    RaftNode<StateMachine, Command>::RaftNode(int node_id, std::vector<RaftNodeConfig> configs) : network_stat(true),
                                                                                                  node_configs(configs),
                                                                                                  my_id(node_id),
                                                                                                  stopped(true),
                                                                                                  role(RaftRole::Follower),
                                                                                                  current_term(0),
                                                                                                  leader_id(-1)
    {
        auto my_config = node_configs[my_id];

        /* launch RPC server */
        rpc_server = std::make_unique<RpcServer>(my_config.ip_address, my_config.port);

        /* Register the RPCs. */
        rpc_server->bind(RAFT_RPC_START_NODE, [this]()
                         { return this->start(); });
        rpc_server->bind(RAFT_RPC_STOP_NODE, [this]()
                         { return this->stop(); });
        rpc_server->bind(RAFT_RPC_CHECK_LEADER, [this]()
                         { return this->is_leader(); });
        rpc_server->bind(RAFT_RPC_IS_STOPPED, [this]()
                         { return this->is_stopped(); });
        rpc_server->bind(RAFT_RPC_NEW_COMMEND, [this](std::vector<u8> data, int cmd_size)
                         { return this->new_command(data, cmd_size); });
        rpc_server->bind(RAFT_RPC_SAVE_SNAPSHOT, [this]()
                         { return this->save_snapshot(); });
        rpc_server->bind(RAFT_RPC_GET_SNAPSHOT, [this]()
                         { return this->get_snapshot(); });

        rpc_server->bind(RAFT_RPC_REQUEST_VOTE, [this](RequestVoteArgs arg)
                         { return this->request_vote(arg); });
        rpc_server->bind(RAFT_RPC_APPEND_ENTRY, [this](RpcAppendEntriesArgs arg)
                         { return this->append_entries(arg); });
        rpc_server->bind(RAFT_RPC_INSTALL_SNAPSHOT, [this](InstallSnapshotArgs arg)
                         { return this->install_snapshot(arg); });

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

        background_election = std::make_unique<std::thread>(&RaftNode::run_background_election, this);
        background_ping = std::make_unique<std::thread>(&RaftNode::run_background_ping, this);
        background_commit = std::make_unique<std::thread>(&RaftNode::run_background_commit, this);
        background_apply = std::make_unique<std::thread>(&RaftNode::run_background_apply, this);

        return 0;
    }

    template <typename StateMachine, typename Command>
    auto RaftNode<StateMachine, Command>::stop() -> int
    {
        /* Lab3: Your code here */
        return 0;
    }

    template <typename StateMachine, typename Command>
    auto RaftNode<StateMachine, Command>::is_leader() -> std::tuple<bool, int>
    {
        /* Lab3: Your code here */
        return std::make_tuple(false, -1);
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
        return std::make_tuple(false, -1, -1);
    }

    template <typename StateMachine, typename Command>
    auto RaftNode<StateMachine, Command>::save_snapshot() -> bool
    {
        /* Lab3: Your code here */
        return true;
    }

    template <typename StateMachine, typename Command>
    auto RaftNode<StateMachine, Command>::get_snapshot() -> std::vector<u8>
    {
        /* Lab3: Your code here */
        return std::vector<u8>();
    }

    /******************************************************************

                             Internal RPC Related

    *******************************************************************/

    template <typename StateMachine, typename Command>
    auto RaftNode<StateMachine, Command>::request_vote(RequestVoteArgs args) -> RequestVoteReply
    {
        /* Lab3: Your code here */
        const auto nodeId = args.node_id_;
        if (args.term_id_ < current_term)
        {
            return {false, current_term};
        }
        if (vote_for_ == nodeId)
        {
            if (args.term_id_ > current_term)
            {
                become_follower(args.term_id_, INVALID_NODE_ID);
                vote_for_ = nodeId;
                return {true, current_term};
            }
            vote_for_ = INVALID_NODE_ID;
            return {false, current_term};
        }
        if (!is_more_up_to_data(args.last_log_term_, args.last_log_idx_))
        {
            if (vote_for_ == my_id)
            {
                vote_gain_--;
            }
            become_follower(args.term_id_, INVALID_NODE_ID);
            vote_for_ = nodeId;
            return {true, current_term};
        }
        return {false, current_term};
    }

    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::handle_request_vote_reply(int target, const RequestVoteArgs arg, const RequestVoteReply reply)
    {
        /* Lab3: Your code here */
        if (reply.term_id_ > current_term)
        {
            current_term = reply.term_id_;
        }
        if (!reply.is_vote_ && reply.term_id_ > current_term)
        {
            become_follower(reply.term_id_, INVALID_NODE_ID);
            vote_for_ = target;
        }
        else if (reply.is_vote_ && role == RaftRole::Candidate)
        {
            current_term = std::max(current_term, reply.term_id_);
            vote_gain_++;
            // RAFT_LOG("get vote from %d, vote gain: %d", target, vote_gain_)
            if (vote_gain_ > node_configs.size() / 2)
            {
                become_leader();
            }
        }
    }

    template <typename StateMachine, typename Command>
    auto RaftNode<StateMachine, Command>::append_entries(RpcAppendEntriesArgs rpc_arg) -> AppendEntriesReply
    {
        /* Lab3: Your code here */
        return AppendEntriesReply();
    }

    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::handle_append_entries_reply(int node_id, const AppendEntriesArgs<Command> arg, const AppendEntriesReply reply)
    {
        /* Lab3: Your code here */
        return;
    }

    template <typename StateMachine, typename Command>
    auto RaftNode<StateMachine, Command>::install_snapshot(InstallSnapshotArgs args) -> InstallSnapshotReply
    {
        /* Lab3: Your code here */
        return InstallSnapshotReply();
    }

    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::handle_install_snapshot_reply(int node_id, const InstallSnapshotArgs arg, const InstallSnapshotReply reply)
    {
        /* Lab3: Your code here */
        return;
    }

    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::send_request_vote(int target_id, RequestVoteArgs arg)
    {
        std::unique_lock<std::mutex> clients_lock(clients_mtx);
        if (rpc_clients_map[target_id] == nullptr || rpc_clients_map[target_id]->get_connection_state() != rpc::client::connection_state::connected)
        {
            return;
        }

        auto res = rpc_clients_map[target_id]->call(RAFT_RPC_REQUEST_VOTE, arg);
        clients_lock.unlock();
        if (res.is_ok())
        {
            handle_request_vote_reply(target_id, arg, res.unwrap()->as<RequestVoteReply>());
        }
        else
        {
            // RPC fails
        }
    }

    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::send_append_entries(int target_id, AppendEntriesArgs<Command> arg)
    {
        std::unique_lock<std::mutex> clients_lock(clients_mtx);
        if (rpc_clients_map[target_id] == nullptr || rpc_clients_map[target_id]->get_connection_state() != rpc::client::connection_state::connected)
        {
            return;
        }

        RpcAppendEntriesArgs rpc_arg = transform_append_entries_args(arg);
        auto res = rpc_clients_map[target_id]->call(RAFT_RPC_APPEND_ENTRY, rpc_arg);
        clients_lock.unlock();
        if (res.is_ok())
        {
            handle_append_entries_reply(target_id, arg, res.unwrap()->as<AppendEntriesReply>());
        }
        else
        {
            // RPC fails
        }
    }

    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::send_install_snapshot(int target_id, InstallSnapshotArgs arg)
    {
        std::unique_lock<std::mutex> clients_lock(clients_mtx);
        if (rpc_clients_map[target_id] == nullptr || rpc_clients_map[target_id]->get_connection_state() != rpc::client::connection_state::connected)
        {
            return;
        }

        auto res = rpc_clients_map[target_id]->call(RAFT_RPC_INSTALL_SNAPSHOT, arg);
        clients_lock.unlock();
        if (res.is_ok())
        {
            handle_install_snapshot_reply(target_id, arg, res.unwrap()->as<InstallSnapshotReply>());
        }
        else
        {
            // RPC fails
        }
    }

    /******************************************************************

                        Tool Functions

    *******************************************************************/

    template <typename StateMachine, typename Command>
    RequestVoteArgs RaftNode<StateMachine, Command>::create_request_vote()
    {
        auto currentTerm = current_term;
        auto [lastLogTerm, lastLogIdx] = log_storage->get_last();
        return {lastLogIdx, lastLogTerm, currentTerm, my_id};
    };

    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::become_follower(const term_id_t term,
                                                          const node_id_t leader)
    {
        role = RaftRole::Follower;
        rand_timer.start();
        current_term = term;
        vote_for_ = INVALID_NODE_ID;
        vote_gain_ = 0;
        leader_id = leader;
    }

    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::become_candidate()
    {
        //  fixed_timer.stop();
        role = RaftRole::Candidate;
        rand_timer.start();
        vote_for_ = my_id;
        vote_gain_ = 1;
    }

    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::become_leader()
    {
        RAFT_LOG("become leader, commit idx: %lu", commit_idx)
        if (role == RaftRole::Leader)
            return;
        role = RaftRole::Leader;
        vote_for_ = INVALID_NODE_ID;
        vote_gain_ = 0;
        leader_id = my_id;
        // initialize the next_idx_map
        auto lastLogIdx = log_storage->size() - 1;
        for (const auto &[idx, cli] : rpc_clients_map)
        {
            next_index_map_[idx] = lastLogIdx + 1;
        }
        // rand_timer.stop();
        fixed_timer.start();
    }

    template <typename StateMachine, typename Command>
    bool RaftNode<StateMachine, Command>::is_more_up_to_data(chfs::term_id_t termId,
                                                             int index)
    {
        auto [lastLogTerm, lastLogIdx] = log_storage->get_last();
        return lastLogTerm > termId || (lastLogTerm == termId && lastLogIdx > index);
    }

    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::recover_from_disk()
    {
        auto res = log_storage->recover();
        // RAFT_LOG("recover, term %d, vote %d", res.first, res.second)
        std::tie(current_term, vote_for_) = res;
    }

    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::recover_state_from_disk()
    {
        // std::lock_guard lockGuard(clients_mtx);
        auto data = log_storage->get_snapshot_data();
        if (data.first == 0)
            return;
        std::stringstream ss;
        ss << data.first;
        for (int i = 0; i < data.first; ++i)
        {
            ss << ' ' << data.second[i];
        }
        std::string str = ss.str();
        std::vector<u8> buffer(str.begin(), str.end());
        state->apply_snapshot(buffer);

        RAFT_LOG(
            "recover state from disk, last-included-term %d, last-included-idx %d, log size: %lu",
            log_storage->get_last_include_idx(), log_storage->get_last_include_term(),
            log_storage->size())
    }

    /******************************************************************

                            Background Workers

    *******************************************************************/

    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::run_background_election()
    {
        while (true)
        {
            {
                if (is_stopped())
                {
                    return;
                }

                std::this_thread::sleep_for(rand_timer.sleep_for());
                if (role == RaftRole::Leader)
                {
                    continue;
                }
                if (!rpc_clients_map[my_id])
                {
                    role = RaftRole::Follower;
                    continue;
                }
                const auto receive = rand_timer.check_receive();
                const auto timeout = rand_timer.timeout();
                if (role == RaftRole::Follower)
                {
                    if (timeout && !receive)
                    {
                        become_candidate();
                    }
                }
                if (!timeout || receive)
                {
                    continue;
                }
                current_term++;
                // RAFT_LOG("term++")
                vote_for_ = my_id;
                vote_gain_ = 1;
                auto args = create_request_vote();
                // RAFT_LOG("new term")
                for (const auto &[idx, cli] : rpc_clients_map)
                {
                    if (idx == my_id)
                        continue;
                    // send_request_vote(idx, args);
                    thread_pool->enqueue(&RaftNode::send_request_vote, this, idx,
                                         args);
                }
            }
        }
    }

    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::run_background_commit()
    {
        // Periodly send logs to the follower.

        // Only work for the leader.

        /* Uncomment following code when you finish */
        // while (true) {
        //     {
        //         if (is_stopped()) {
        //             return;
        //         }
        //         /* Lab3: Your code here */
        //     }
        // }

        return;
    }

    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::run_background_apply()
    {
        // Periodly apply committed logs the state machine

        // Work for all the nodes.

        /* Uncomment following code when you finish */
        // while (true) {
        //     {
        //         if (is_stopped()) {
        //             return;
        //         }
        //         /* Lab3: Your code here */
        //     }
        // }

        return;
    }

    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::run_background_ping()
    {
        // Periodly send empty append_entries RPC to the followers.

        // Only work for the leader.

        /* Uncomment following code when you finish */
        // while (true) {
        //     {
        //         if (is_stopped()) {
        //             return;
        //         }
        //         /* Lab3: Your code here */
        //     }
        // }

        return;
    }

    /******************************************************************

                              Test Functions (must not edit)

    *******************************************************************/

    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::set_network(std::map<int, bool> &network_availability)
    {
        std::unique_lock<std::mutex> clients_lock(clients_mtx);

        /* turn off network */
        if (!network_availability[my_id])
        {
            for (auto &&client : rpc_clients_map)
            {
                if (client.second != nullptr)
                    client.second.reset();
            }

            return;
        }

        for (auto node_network : network_availability)
        {
            int node_id = node_network.first;
            bool node_status = node_network.second;

            if (node_status && rpc_clients_map[node_id] == nullptr)
            {
                RaftNodeConfig target_config;
                for (auto config : node_configs)
                {
                    if (config.node_id == node_id)
                        target_config = config;
                }

                rpc_clients_map[node_id] = std::make_unique<RpcClient>(target_config.ip_address, target_config.port, true);
            }

            if (!node_status && rpc_clients_map[node_id] != nullptr)
            {
                rpc_clients_map[node_id].reset();
            }
        }
    }

    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::set_reliable(bool flag)
    {
        std::unique_lock<std::mutex> clients_lock(clients_mtx);
        for (auto &&client : rpc_clients_map)
        {
            if (client.second)
            {
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

        for (auto &&client : rpc_clients_map)
        {
            if (client.second)
            {
                sum += client.second->count();
            }
        }

        return sum;
    }

    template <typename StateMachine, typename Command>
    std::vector<u8> RaftNode<StateMachine, Command>::get_snapshot_direct()
    {
        if (is_stopped())
        {
            return std::vector<u8>();
        }

        std::unique_lock<std::mutex> lock(mtx);

        return state->snapshot();
    }

}
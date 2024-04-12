#pragma once

#include "block/manager.h"
#include <mutex>
#include <vector>
#include <cstring>
#include <random>
#include "rsm/list_state_machine.h"

namespace chfs
{
    using term_id_t = int;
    using node_id_t = int;
    using commit_id_t = unsigned long;

    constexpr int MAGIC_NUM = 123123123;

    template <typename Command>
    struct LogEntry
    {
        term_id_t term_id_;
        Command command_;

        LogEntry(term_id_t term, Command cmd)
            : term_id_(term), command_(cmd)
        {
        }

        LogEntry()
            : term_id_(0)
        {
            command_ = ListCommand(0);
        }
    };

    /**
     * RaftLog uses a BlockManager to manage the data..
     */
    template <typename Command>
    class RaftLog
    {
    public:
        RaftLog(std::shared_ptr<BlockManager> bm);
        ~RaftLog();
        [[nodiscard]] size_t size();

        // check whether the log is valid
        bool validate_log(term_id_t last_log_term, commit_id_t last_log_idx);
        // insert
        // void insert_or_rewrite(LogEntry<Command> &entry, int idx);
        commit_id_t append_log(term_id_t term, Command command);
        void insert(term_id_t term, Command command);
        // get
        std::pair<term_id_t, commit_id_t> get_last();
        // LogEntry<Command> get_nth(int n);
        // std::vector<LogEntry<Command>> get_after_nth(int n);
        // delete
        void delete_after_nth(int n);
        void delete_before_nth(int n);

        /* persist */
        void persist(term_id_t current_term, int vote_for);
        void write_meta(term_id_t current_term, int vote_for);
        void write_data();
        std::pair<term_id_t, int> recover();
        std::tuple<term_id_t, int, int> get_meta();
        void get_data(int size);

        /* snapshot */
        [[nodiscard]] int get_last_include_idx() const { return last_include_idx_; }

        [[nodiscard]] term_id_t get_last_include_term() const
        {
            return last_inclued_term_;
        }

        [[nodiscard]] std::pair<int, std::vector<int>> get_snapshot_data() const;
        std::vector<u8> create_snap(int commit_idx);
        void write_snapshot(
            int offset,
            std::vector<u8> data);
        void last_snapshot(term_id_t last_included_term,
                           int last_included_idx);

        /* Lab3: Your code here */

    private:
        std::shared_ptr<BlockManager> bm_;
        std::shared_ptr<BlockManager> log_meta_bm_, log_data_bm_, snap_bm_;
        std::mutex mtx;
        /* Lab3: Your code here */
        std::vector<LogEntry<Command>> logs_;
        int meta_str_size;
        int per_entry_size;
        int snapshot_idx_ = 0;
        node_id_t node_id_;
        term_id_t last_inclued_term_ = 0;
        int last_include_idx_ = 0;
    };

    template <typename Command>
    RaftLog<Command>::RaftLog(std::shared_ptr<BlockManager> bm)
    {
        /* Lab3: Your code here */
    }

    template <typename Command>
    RaftLog<Command>::~RaftLog()
    {
        /* Lab3: Your code here */
    }

    template <typename Command>
    std::pair<term_id_t, commit_id_t> RaftLog<Command>::get_last()
    {
        std::lock_guard lockGuard(mtx);
        if (logs_.size() <= 1)
        {
            return {last_inclued_term_, last_include_idx_};
        }
        return {logs_.back().term_id_, logs_.size() - 1};
    }

    template <typename Command>
    size_t RaftLog<Command>::size()
    {
        std::lock_guard lockGuard(mtx);
        return logs_.size();
    }

    /* Lab3: Your code here */
    class IntervalTimer
    {
    public:
        explicit IntervalTimer(const long long interval)
        {
            fixed = true;
            start_time = std::chrono::steady_clock::now();
            gen = std::mt19937(rd());
            dist = std::uniform_int_distribution(500, 550);
            interval_ = interval;
        }

        IntervalTimer()
        {
            start_time = std::chrono::steady_clock::now();
            gen = std::mt19937(rd());
            dist = std::uniform_int_distribution(500, 550);
            interval_ = dist(gen);
        }

        [[nodiscard]] std::chrono::milliseconds sleep_for() const
        {
            return std::chrono::milliseconds(interval_);
        }

        void start()
        {
            started_ = true;
            start_time = std::chrono::steady_clock::now();
        }

        void stop()
        {
            started_.store(false);
        };

        void reset()
        {
            start_time = std::chrono::steady_clock::now();
            receive_from_leader_ = false;
            if (fixed)
            {
                return;
            }
            interval_ = dist(gen);
        }

        void receive() { receive_from_leader_.store(true); }
        bool check_receive() const { return receive_from_leader_.load(); }

        bool timeout()
        {
            if (started_)
            {
                curr_time = std::chrono::steady_clock::now();
                if (const auto duration = std::chrono::duration_cast<
                                              std::chrono::milliseconds>(
                                              curr_time - start_time)
                                              .count();
                    duration > interval_)
                {
                    reset();
                    return true;
                }
                reset();
            }
            return false;
        };

    private:
        std::mutex mtx;
        long long interval_;
        bool fixed = false;
        std::atomic<bool> started_ = false;
        std::atomic<bool> receive_from_leader_ = false;
        std::chrono::steady_clock::time_point start_time;
        std::chrono::steady_clock::time_point curr_time;
        std::random_device rd;
        std::mt19937 gen;
        std::uniform_int_distribution<int> dist;
    };
} /* namespace chfs */

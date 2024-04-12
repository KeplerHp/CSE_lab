//===----------------------------------------------------------------------===//
//
//                         Chfs
//
// commit_log.h
//
// Identification: src/include/distributed/commit_log.h
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "block/manager.h"
#include "common/config.h"
#include "common/macros.h"
#include "filesystem/operations.h"
#include <atomic>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <unordered_set>
#include <vector>

namespace chfs {
/**
 * `BlockOperation` is an entry indicates an old block state and
 * a new block state. It's used to redo the operation when
 * the system is crashed.
 */
class BlockOperation {
public:
  explicit BlockOperation(block_id_t block_id, std::vector<u8> new_block_state)
      : block_id_(block_id), new_block_state_(new_block_state) {
    CHFS_ASSERT(new_block_state.size() == DiskBlockSize, "invalid block state");
  }

  block_id_t block_id_;
  std::vector<u8> new_block_state_;
};

/**
 * `LogTransformer` is an abstract class that transform a metadata
 * operation into a list of block edits. It's used to record the
 * block edits into the commit log.
 */
class LogTransformer {
public:
  explicit LogTransformer(
      std::map<inode_id_t, std::shared_ptr<std::shared_mutex>> inode_locks,
      std::shared_ptr<FileOperation> op);

  auto transform_mknode(u8 type, inode_id_t parent, const std::string &name)
      -> std::pair<txn_id_t, std::vector<std::shared_ptr<BlockOperation>>>;

  auto transform_unlink(inode_id_t parent, const std::string &name)
      -> std::pair<txn_id_t, std::vector<std::shared_ptr<BlockOperation>>>;

private:
  auto transform_free_inode(inode_id_t id)
      -> std::vector<std::shared_ptr<BlockOperation>>;

  auto transform_free_block(block_id_t id) -> std::shared_ptr<BlockOperation>;

  auto transform_write_file(inode_id_t id, const std::vector<u8> &data)
      -> std::vector<std::shared_ptr<BlockOperation>>;

  auto transform_alloc_inode(u8 type)
      -> std::pair<inode_id_t, std::vector<std::shared_ptr<BlockOperation>>>;

  auto transform_alloc_block()
      -> std::pair<block_id_t, std::shared_ptr<BlockOperation>>;

  std::shared_ptr<FileOperation> operation_;
  std::atomic<txn_id_t> current_txn_id_;
  // Concurrent related
  std::shared_ptr<std::shared_mutex> inode_bitmap_lock;
  std::map<block_id_t, std::shared_ptr<std::shared_mutex>> inode_table_locks;
  std::shared_ptr<std::shared_mutex> bitmap_lock;
  std::map<inode_id_t, std::shared_ptr<std::shared_mutex>> inode_locks;
};

/**
 * `CommitLog` is a class that records the block edits into the
 * commit log. It's used to redo the operation when the system
 * is crashed.
 */
class CommitLog {
public:
  explicit CommitLog(std::shared_ptr<BlockManager> bm,
                     bool is_checkpoint_enabled);
  ~CommitLog();
  auto append_log(txn_id_t txn_id,
                  std::vector<std::shared_ptr<BlockOperation>> ops) -> void;
  auto commit_log(txn_id_t txn_id) -> void;
  auto checkpoint() -> void;
  auto recover() -> void;
  auto get_log_entry_num() -> usize;

private:
  auto append_log_inner(std::vector<std::shared_ptr<BlockOperation>> ops)
      -> void;
  std::mutex mutex_;         // Protect persist log
  usize log_cnt;             // When to trigger checkpoint
  block_id_t last_block_id_; // where to write next log
  block_id_t log_block_start_index_;
  [[maybe_unused]] bool is_checkpoint_enabled_;
  std::shared_ptr<BlockManager> bm_;
};

} // namespace chfs
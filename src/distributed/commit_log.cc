#include <algorithm>

#include "common/bitmap.h"
#include "distributed/commit_log.h"
#include "distributed/metadata_server.h"
#include "filesystem/directory_op.h"
#include "metadata/inode.h"
#include <sys/mman.h>
#include <chrono>

namespace chfs
{
  /**
   * `CommitLog` part
   */
  // {Your code here}
  CommitLog::CommitLog(std::shared_ptr<BlockManager> bm,
                       bool is_checkpoint_enabled)
      : is_checkpoint_enabled_(is_checkpoint_enabled), bm_(bm)
  {
  }

  CommitLog::~CommitLog() {}

  // {Your code here}
  auto CommitLog::get_log_entry_num() -> usize
  {
    // TODO: Implement this function.
    // UNIMPLEMENTED();
    return this->tx_list.size();
  }

  // {Your code here}
  auto CommitLog::append_log(txn_id_t txn_id,
                             std::vector<std::shared_ptr<BlockOperation> > ops)
      -> void
  {
    // TODO: Implement this function.
    // UNIMPLEMENTED();
    for (auto &op : ops)
    {
      u32 i;
      for (i = 0; i < 1024; i++)
      {
        if (this->used_blocks.find(i) == used_blocks.end())
        {
          used_blocks.insert(i);
          break;
        }
      }
      auto ptr = this->bm_->unsafe_get_block_ptr();
      memcpy(&ptr[(bm_->total_blocks() + i) * DiskBlockSize], op->new_block_state_.data(), DiskBlockSize);

      msync(&ptr[(bm_->total_blocks() + i) * DiskBlockSize], DiskBlockSize, MS_SYNC | MS_INVALIDATE);

      this->tx_list.push_back(txn_id);
      this->op_maps[txn_id].emplace_back(std::pair<block_id_t, block_id_t>(op->block_id_, i + bm_->total_blocks()));
    }
  }

  // {Your code here}
  auto CommitLog::commit_log(txn_id_t txn_id, bool wt_success) -> void
  {
    // TODO: Implement this function.
    // UNIMPLEMENTED();
    if (wt_success)
      checkpoint();
    else
    {
      auto res = this->op_maps[txn_id];
      auto ptr = this->bm_->unsafe_get_block_ptr();
      for (auto &o : res)
      {
        memcpy(&ptr[DiskBlockSize * o.first], &ptr[DiskBlockSize * o.second], DiskBlockSize);
        msync(&ptr[DiskBlockSize * o.first], DiskBlockSize, MS_SYNC | MS_INVALIDATE);
      }
      checkpoint();
    }
  }

  // {Your code here}
  auto CommitLog::checkpoint() -> void
  {
    // TODO: Implement this function.
    // UNIMPLEMENTED();
    for (auto i : tx_list)
    {
      for (auto &op : this->op_maps[i])
      {
        used_blocks.erase(op.second - this->bm_->total_blocks());
      }
      this->op_maps[i].clear();
    }
    tx_list.clear();
  }

  // {Your code here}
  auto CommitLog::recover() -> void
  {
    // TODO: Implement this function.
    // UNIMPLEMENTED();
    auto ptr = this->bm_->unsafe_get_block_ptr();
    for (auto &tx : tx_list)
    {
      for (auto &o : this->op_maps[tx])
      {
        memcpy(&ptr[DiskBlockSize * o.first], &ptr[DiskBlockSize * o.second], DiskBlockSize);
        msync(&ptr[DiskBlockSize * o.first], DiskBlockSize, MS_SYNC | MS_INVALIDATE);
      }
    }
  }
}; // namespace chfs
//===----------------------------------------------------------------------===//
//
//                         Chfs
//
// manager.h
//
// Identification: src/include/metadata/manager.h
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "./inode.h"
#include "block/allocator.h"
#include "distributed/commit_log.h"
#include <map>
#include <shared_mutex>

namespace chfs {

// inode should be larger than 0
const inode_id_t KInvalidInodeID = 0;

class FileOperation;

/**
 * The manager manages the inode.
 * This includes managing an inode table.
 *
 * At a high-level, the inode manager assumes the following layout on blocks:
 * | block 0     | block 1  ...  | block x ...             | block y ... | block
 * N  ...         |
 * | Super block | Inode Table   | Inode allocation bitmap |
 * Block allocation bitmap ... |  Other data blocks   |
 * 
 * The inode manager is also not thread-safe. You should implement it to be 
 * thread-safe in lab2.
 */
class InodeManager {
  friend class FileOperation;
  friend class LogTransformer;
  // We will modify the block manager
  std::shared_ptr<BlockManager> bm;
  u64 max_inode_supported;
  u64 n_table_blocks;
  u64 n_bitmap_blocks;
  // Concurrent related
  std::shared_ptr<std::shared_mutex> inode_bitmap_lock;
  std::map<block_id_t, std::shared_ptr<std::shared_mutex>> inode_table_locks;
  bool is_log_enabled;

public:
  /**
   * Construct an InodeManager from scratch.
   * Note that it will initialize the blocks in the block manager.
   */
  InodeManager(std::shared_ptr<BlockManager> bm, u64 max_inode_supported);

  static auto to_shared_ptr(InodeManager m) -> std::shared_ptr<InodeManager> {
    return std::make_shared<InodeManager>(m);
  }

  /**
   * Construct an InodeManager from a block manager.
   * Note that it won't modify any blocks on the block manager.
   *
   * The max_inode_supported can be found in the super block.
   */
  static auto create_from_block_manager(std::shared_ptr<BlockManager> bm,
                                        u64 max_inode_supported)
      -> ChfsResult<InodeManager>;

  /**
   * Get the maximum number of inode supported.
   * The number is determined when the file system is created.
   */
  auto get_max_inode_supported() const -> u64 { return max_inode_supported; }

  /**
   * Allocate and initialize an inode with proper type
   * @param type: file type
   * @param bid: inode block ID
   */
  auto allocate_inode(InodeType type, block_id_t bid) -> ChfsResult<inode_id_t>;

  /**
   * Get the number of free inodes
   * @return the number of free inodes if Ok
   */
  auto free_inode_cnt() const -> ChfsResult<u64>;

  /**
   * Get the block ID of the inode
   * @param id: **logical** inode ID
   *
   * Note that we don't check whether the returned block id is valid
   */
  auto get(inode_id_t id) -> ChfsResult<block_id_t>;

  /**
   * Free the inode entry id
   */
  auto free_inode(inode_id_t id) -> ChfsNullResult;

  /**
   * Get the attribute of the inode
   */
  auto get_attr(inode_id_t id) -> ChfsResult<FileAttr>;

  /**
   * Set the type of the inode
   */
  auto get_type(inode_id_t id) -> ChfsResult<InodeType>;

  /**
   * The combined version of the above APIs
   */
  auto get_type_attr(inode_id_t id)
      -> ChfsResult<std::pair<InodeType, FileAttr>>;

  auto get_reserved_blocks() const -> usize {
    return 1 + n_table_blocks + n_bitmap_blocks;
  }

  auto set_log_enabled(bool enabled) -> ChfsNullResult {
    is_log_enabled = enabled;
    return KNullOk;
  }

  // helper functions

  /**
   * Set the block ID of the inode
   * @param idx: **physical** inode ID
   */
  auto set_table(inode_id_t idx, block_id_t bid) -> ChfsNullResult;

private:
  /**
   * Simple constructors
   */
  InodeManager(std::shared_ptr<BlockManager> bm, u64 max_inode_supported,
               u64 ntables, u64 nbit)
      : bm(bm), max_inode_supported(max_inode_supported),
        n_table_blocks(ntables), n_bitmap_blocks(nbit), is_log_enabled(false) {
          inode_bitmap_lock = std::make_shared<std::shared_mutex>();
          for (u64 i = 0; i < ntables; ++i)
            inode_table_locks.insert({i + 1, std::make_shared<std::shared_mutex>()});
        }

  /**
   * Read the inode to a buffer
   * @param block_id_t: the block id that stores the inode
   */
  auto read_inode(inode_id_t id, std::vector<u8> &buffer)
      -> ChfsResult<block_id_t>;
};

} // namespace chfs
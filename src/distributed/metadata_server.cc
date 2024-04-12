#include "distributed/metadata_server.h"
#include "common/util.h"
#include "filesystem/directory_op.h"
#include <fstream>

namespace chfs {
std::mutex mtx_tx;
std::vector<std::tuple<block_id_t, std::vector<u8>, bool>> record;

inline auto MetadataServer::bind_handlers() {
  server_->bind("mknode",
                [this](u8 type, inode_id_t parent, std::string const &name) {
                  return this->mknode(type, parent, name);
                });
  server_->bind("unlink", [this](inode_id_t parent, std::string const &name) {
    return this->unlink(parent, name);
  });
  server_->bind("lookup", [this](inode_id_t parent, std::string const &name) {
    return this->lookup(parent, name);
  });
  server_->bind("get_block_map",
                [this](inode_id_t id) { return this->get_block_map(id); });
  server_->bind("alloc_block",
                [this](inode_id_t id) { return this->allocate_block(id); });
  server_->bind("free_block",
                [this](inode_id_t id, block_id_t block, mac_id_t machine_id) {
                  return this->free_block(id, block, machine_id);
                });
  server_->bind("readdir", [this](inode_id_t id) { return this->readdir(id); });
  server_->bind("get_type_attr",
                [this](inode_id_t id) { return this->get_type_attr(id); });
}

inline auto MetadataServer::init_fs(const std::string &data_path) {
  /**
   * Check whether the metadata exists or not.
   * If exists, we wouldn't create one from scratch.
   */
  bool is_initialed = is_file_exist(data_path);

  auto block_manager = std::shared_ptr<BlockManager>(nullptr);
  if (is_log_enabled_) {
    block_manager =
        std::make_shared<BlockManager>(data_path, KDefaultBlockCnt, true);
  } else {
    block_manager = std::make_shared<BlockManager>(data_path, KDefaultBlockCnt);
  }

  CHFS_ASSERT(block_manager != nullptr, "Cannot create block manager.");

  if (is_initialed) {
    auto origin_res = FileOperation::create_from_raw(block_manager);
    std::cout << "Restarting..." << std::endl;
    if (origin_res.is_err()) {
      std::cerr << "Original FS is bad, please remove files manually."
                << std::endl;
      exit(1);
    }

    operation_ = origin_res.unwrap();
  } else {
    operation_ = std::make_shared<FileOperation>(block_manager,
                                                 DistributedMaxInodeSupported);
    std::cout << "We should init one new FS..." << std::endl;
    /**
     * If the filesystem on metadata server is not initialized, create
     * a root directory.
     */
    auto init_res = operation_->alloc_inode(InodeType::Directory);
    if (init_res.is_err()) {
      std::cerr << "Cannot allocate inode for root directory." << std::endl;
      exit(1);
    }

    CHFS_ASSERT(init_res.unwrap() == 1, "Bad initialization on root dir.");
  }

  this->mutex_ = std::make_shared<InodeMutex>(this->operation_->inode_manager_->get_max_inode_supported());

  running = false;
  num_data_servers =
      0; // Default no data server. Need to call `reg_server` to add.

  if (is_log_enabled_) {
    if (may_failed_)
      operation_->block_manager_->set_may_fail(true);
    commit_log = std::make_shared<CommitLog>(operation_->block_manager_,
                                             is_checkpoint_enabled_);
  }

  bind_handlers();

  /**
   * The metadata server wouldn't start immediately after construction.
   * It should be launched after all the data servers are registered.
   */
}

MetadataServer::MetadataServer(u16 port, const std::string &data_path,
                               bool is_log_enabled, bool is_checkpoint_enabled,
                               bool may_failed)
    : is_log_enabled_(is_log_enabled), may_failed_(may_failed),
      is_checkpoint_enabled_(is_checkpoint_enabled) {
  server_ = std::make_unique<RpcServer>(port);
  init_fs(data_path);
  if (is_log_enabled_) {
    commit_log = std::make_shared<CommitLog>(operation_->block_manager_,
                                             is_checkpoint_enabled);
  }
}

MetadataServer::MetadataServer(std::string const &address, u16 port,
                               const std::string &data_path,
                               bool is_log_enabled, bool is_checkpoint_enabled,
                               bool may_failed)
    : is_log_enabled_(is_log_enabled), may_failed_(may_failed),
      is_checkpoint_enabled_(is_checkpoint_enabled) {
  server_ = std::make_unique<RpcServer>(address, port);
  init_fs(data_path);
  if (is_log_enabled_) {
    commit_log = std::make_shared<CommitLog>(operation_->block_manager_,
                                             is_checkpoint_enabled);
  }
}

// {Your code here}
auto MetadataServer::mknode(u8 type, inode_id_t parent, const std::string &name)
    -> inode_id_t {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  this->mutex_->lock_all();
  this->mutex_->lock_inode(parent);
  bool failed = false;
  // std::cout << "mknode in metadata server & locked: " << parent << std::endl;
  if (type == RegularFileType) {
    auto res = this->operation_->mkfile(parent, name.c_str());
    if (this->is_log_enabled_) {
      mtx_tx.lock();
      std::vector<std::shared_ptr<BlockOperation>> vec;
      for (auto &log : record) {
        vec.emplace_back(std::make_shared<BlockOperation>(std::get<0>(log), std::get<1>(log)));
        if (std::get<2>(log) == true)
          failed = true;
      }
      auto tx_id = this->commit_log->txn_id.load();
      this->commit_log->append_log(this->commit_log->txn_id++, vec);
      this->commit_log->commit_log(tx_id, !failed);
      record.clear();
      mtx_tx.unlock();
    }

    if (res.is_err ()) {
      this->mutex_->release_inode(parent);
      this->mutex_->release_all();
      return 0;
    }
    this->mutex_->release_inode(parent);
    this->mutex_->release_all();
    if (failed)
      return 0;

    return res.unwrap();
  } else if (type == DirectoryType) {
    auto res = this->operation_->mkdir(parent, name.c_str());

    if (this->is_log_enabled_) {
      mtx_tx.lock();
      std::vector<std::shared_ptr<BlockOperation>> vec;
      for (auto &log : record) {
        vec.emplace_back(std::make_shared<BlockOperation>(std::get<0>(log), std::get<1>(log)));
        if (std::get<2>(log) == true)
          failed = true;
      }
      auto tx_id = this->commit_log->txn_id.load();
      this->commit_log->append_log(this->commit_log->txn_id++, vec);
      this->commit_log->commit_log(tx_id, !failed);
      record.clear();
      mtx_tx.unlock();
    }

    if (res.is_err()) {
      this->mutex_->release_inode(parent);
      this->mutex_->release_all();
      return 0;
    }
    this->mutex_->release_inode(parent);
    this->mutex_->release_all();
    if (failed) {
      return 0;
    }
    
    return res.unwrap();
  }

  this->mutex_->release_inode(parent);
  this->mutex_->release_all();

  return 0;
}

// {Your code here}
auto MetadataServer::unlink(inode_id_t parent, const std::string &name)
    -> bool {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  this->mutex_->lock_all();

  this->mutex_->lock_inode(parent);
  // std::cout << "unlink in metadata server & locked: " << parent << std::endl;

  auto res_lk = this->operation_->lookup(parent, name.c_str());
  if (res_lk.is_err()) {
    this->mutex_->release_inode(parent);
    return false;
  }
  auto res = res_lk.unwrap();
  // auto res = this->lookup(parent, name.c_str());
  // get file inode num to be removed
  this->mutex_->lock_inode(res);

  bool failed = false;

  std::vector<u8> inode(this->operation_->block_manager_->block_size());
  auto blk_id = this->operation_->inode_manager_->get(res);
  if (blk_id.is_err()) {
    this->mutex_->release_inode(res);
    this->mutex_->release_inode(parent);
    this->mutex_->release_all();           /////////////
    return false;
  }
  this->operation_->block_manager_->read_block(blk_id.unwrap(), inode.data());
  auto inode_p = reinterpret_cast<Inode *>(inode.data()); // read file inode

  auto dir_res = this->operation_->read_file(parent);
  if (dir_res.is_err()) {
    this->mutex_->release_inode(res);
    this->mutex_->release_inode(parent);
    this->mutex_->release_all();           /////////////
    return false;
  } // read parent

  if (inode_p->get_type() == chfs::InodeType::FILE) {
    auto blk_cntt = (DiskBlockSize - sizeof(Inode)) / 12UL;
    auto blk_p = &(inode_p->blocks[0]);
    auto mac_p = (mac_id_t *)&(inode_p->blocks[blk_cntt]);
    std::cout << "blkcc: " << blk_cntt << " nblk: " << inode_p->get_nblocks();
    for (u32 i = 0; i < blk_cntt; i++) {
      if (blk_p[i] == 0) {
        continue;
      } else {
        // this->free_block(res, blk_p[i], mac_p[i]);
        this->mutex_->lock_mach(mac_p[i]);
        this->clients_[mac_p[i]]->call("free_block", blk_p[i]);
        this->mutex_->release_mac(mac_p[i]);
      }
    }
    this->operation_->inode_manager_->free_inode(res);
    this->operation_->block_allocator_->deallocate(blk_id.unwrap());
    
    /*if file free block & inode*/
  } else if (inode_p->get_type() == chfs::InodeType::Directory) {
    std::list<DirectoryEntry> list_self;
    chfs::read_directory(this->operation_.get(), res, list_self);
    if (!list_self.empty()) {
      this->mutex_->release_inode(res);
      this->mutex_->release_inode(parent);
      this->mutex_->release_all();              ////////////////
      return false;
    }
    this->operation_->remove_file(res);

  }

  auto res_data = dir_res.unwrap();
  std::string str(res_data.begin(), res_data.end());
  str = chfs::rm_from_directory(str, name.c_str());
  std::vector<u8> vec(str.begin(), str.end());
  this->operation_->write_file(parent, vec);
  
  if (this->is_log_enabled_) {
    mtx_tx.lock();
    std::vector<std::shared_ptr<BlockOperation>> BO;
    for (auto &log : record) {
      BO.emplace_back(std::make_shared<BlockOperation>(std::get<0>(log), std::get<1>(log)));
      if (std::get<2>(log) == true)
          failed = true;
    }
    auto tx_id = this->commit_log->txn_id.load();
    this->commit_log->append_log(this->commit_log->txn_id++, BO);
    this->commit_log->commit_log(tx_id, !failed);
    record.clear();
    mtx_tx.unlock();
  }
  
  this->mutex_->release_inode(res);
  this->mutex_->release_inode(parent);
  this->mutex_->release_all();          /////////////
  return true;
  /*rm from parent*/

  //return false;
}

// {Your code here}
auto MetadataServer::lookup(inode_id_t parent, const std::string &name)
    -> inode_id_t {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  this->mutex_->lock_inode(parent);
  auto res = this->operation_->lookup(parent, name.c_str());
  if (res.is_ok()) {
    this->mutex_->release_inode(parent);
    return res.unwrap();
  }
  this->mutex_->release_inode(parent);
  return 0;
}

// {Your code here}
auto MetadataServer::get_block_map(inode_id_t id) -> std::vector<BlockInfo> {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  this->mutex_->lock_inode(id);

  std::vector<u8> inode(this->operation_->block_manager_->block_size());
  auto inode_p = reinterpret_cast<Inode *>(inode.data());
  auto blk_id = this->operation_->inode_manager_->get(id);
  this->operation_->block_manager_->read_block(blk_id.unwrap(), inode.data());

  auto blk_cntt = (DiskBlockSize - sizeof(Inode)) / 12UL;
  auto blk_p = &(inode_p->blocks[0]);
  auto mac_p = (mac_id_t *)&(inode_p->blocks[blk_cntt]);
  std::cout << "blkcc: " << blk_cntt << " nblk: " << inode_p->get_nblocks();
  std::vector<BlockInfo> blockinfo;

  for (u32 i = 0; i < blk_cntt; i++) {
    if (blk_p[i] == 0) {
      continue;
    } else {
      auto res = this->clients_[mac_p[i]]->call("block_version", blk_p[i]);
      blockinfo.emplace_back(BlockInfo(blk_p[i], mac_p[i], res.unwrap()->as<version_t>()));
    }
  }
  
  this->mutex_->release_inode(id);
  return blockinfo;
}

// {Your code here}
auto MetadataServer::allocate_block(inode_id_t id) -> BlockInfo {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  this->mutex_->lock_inode(id);

  auto mac_id = this->generator.rand(1, this->num_data_servers);

  this->mutex_->lock_mach(mac_id);
  auto res = this->clients_[mac_id]->call("alloc_block");
  this->mutex_->release_mac(mac_id);
  
  auto res_str = res.unwrap()->as<std::pair<block_id_t, version_t>>();
  if (res_str.first == 0) {
    this->mutex_->release_inode(id);
    return {0, 0, 0};
  }
  std::vector<u8> inode(this->operation_->block_manager_->block_size());
  auto inode_p = reinterpret_cast<Inode *>(inode.data());
  auto blk_id = this->operation_->inode_manager_->get(id);
  this->operation_->block_manager_->read_block(blk_id.unwrap(), inode.data());
  
  auto blk_cntt = (DiskBlockSize - sizeof(Inode)) / 12UL;
  auto blk_p = &(inode_p->blocks[0]);
  auto mac_p = (mac_id_t *)&(inode_p->blocks[blk_cntt]);
  std::cout << "blkcc: " << blk_cntt << " nblk: " << inode_p->get_nblocks();
  for (u32 i = 0; i < blk_cntt; i++) {
    if (blk_p[i] == 0) {
      blk_p[i] = res_str.first;
      mac_p[i] = mac_id;
      break;
    }
  }
  inode_p->set_size(inode_p->get_size() + DiskBlockSize);
  this->operation_->block_manager_->write_block(blk_id.unwrap(), inode.data());

  this->mutex_->release_inode(id);

  return {res_str.first, mac_id, res_str.second};
}

// {Your code here}
auto MetadataServer::free_block(inode_id_t id, block_id_t block_id,
                                mac_id_t machine_id) -> bool {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  this->mutex_->lock_inode(id);

  auto blk_id = operation_->inode_manager_->get(id).unwrap();
  std::vector<u8> blo_node(operation_->block_manager_->block_size());
  this->operation_->block_manager_->read_block(blk_id, blo_node.data());
  auto inode_p = reinterpret_cast<Inode *>(blo_node.data());

  auto blk_cntt = (DiskBlockSize - sizeof(Inode)) / 12UL;
  auto blk_p = &(inode_p->blocks[0]);
  auto mac_p = (mac_id_t *)&(inode_p->blocks[blk_cntt]);
  std::cout << "blkcc: " << blk_cntt << " nblk: " << inode_p->get_nblocks();
  bool isTrue = false;

  for (u32 i = 0; i < blk_cntt; i++) {
    if (blk_p[i] == block_id && mac_p[i] == machine_id) {
      blk_p[i] = 0;
      mac_p[i] = 0;
      isTrue = true;
      break;
    }
  }
  inode_p->set_size(inode_p->get_size() - DiskBlockSize);
  this->operation_->block_manager_->write_block(blk_id, blo_node.data());

  this->mutex_->lock_mach(machine_id);
  auto res = this->clients_[machine_id]->call("free_block", block_id);
  this->mutex_->release_mac(machine_id);
  auto res_bool = res.unwrap()->as<bool>();
  if (res_bool == false) {
    isTrue = false;
  }

  this->mutex_->release_inode(id);
  return isTrue;
}

// {Your code here}
auto MetadataServer::readdir(inode_id_t node)
    -> std::vector<std::pair<std::string, inode_id_t>> {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  this->mutex_->lock_inode(node);
  std::list<chfs::DirectoryEntry> list;
  read_directory(this->operation_.get(), node, list);
  std::vector<std::pair<std::string, inode_id_t>> vec;
  for (auto &item : list) {
    vec.emplace_back(std::pair<std::string, inode_id_t>(item.name, item.id));
  }

  this->mutex_->release_inode(node);

  return vec;
}

// {Your code here}
auto MetadataServer::get_type_attr(inode_id_t id)
    -> std::tuple<u64, u64, u64, u64, u8> {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  this->mutex_->lock_inode(id);
  auto res = this->operation_->inode_manager_->get_type_attr(id).unwrap();
  this->mutex_->release_inode(id);
  return {res.second.size, res.second.atime, res.second.mtime, res.second.ctime, (u8)res.first};
}

auto MetadataServer::reg_server(const std::string &address, u16 port,
                                bool reliable) -> bool {
  num_data_servers += 1;
  auto cli = std::make_shared<RpcClient>(address, port, reliable);
  clients_.insert(std::make_pair(num_data_servers, cli));
  this->mutex_->insert_mac(num_data_servers);

  return true;
}

auto MetadataServer::run() -> bool {
  if (running)
    return false;

  // Currently we only support async start
  server_->run(true, num_worker_threads);
  running = true;
  return true;
}

} // namespace chfs
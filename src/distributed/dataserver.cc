#include "distributed/dataserver.h"
#include "common/util.h"

namespace chfs {

auto DataServer::initialize(std::string const &data_path) {
  /**
   * At first check whether the file exists or not.
   * If so, which means the distributed chfs has
   * already been initialized and can be rebuilt from
   * existing data.
   */
  bool is_initialized = is_file_exist(data_path);
  usize version_blk_num = KDefaultBlockCnt / (DiskBlockSize / sizeof(version_t));
  auto bm = std::shared_ptr<BlockManager>(
      new BlockManager(data_path, KDefaultBlockCnt));
  if (is_initialized) {
    block_allocator_ =
        std::make_shared<BlockAllocator>(bm, version_blk_num, false);
  } else {
    // We need to reserve some blocks for storing the version of each block
    block_allocator_ = std::shared_ptr<BlockAllocator>(
        new BlockAllocator(bm, version_blk_num, true));
  }

  for (u32 i = 0; i < version_blk_num; i++) {
    this->block_allocator_->bm->zero_block(i);
  }

  // Initialize the RPC server and bind all handlers
  server_->bind("block_version", [this](block_id_t block_id){
    return this->get_blk_version(block_id);
  });
  server_->bind("read_data", [this](block_id_t block_id, usize offset,
                                    usize len, version_t version) {
    return this->read_data(block_id, offset, len, version);
  });
  server_->bind("write_data", [this](block_id_t block_id, usize offset,
                                     std::vector<u8> &buffer) {
    return this->write_data(block_id, offset, buffer);
  });
  server_->bind("alloc_block", [this]() { return this->alloc_block(); });
  server_->bind("free_block", [this](block_id_t block_id) {
    return this->free_block(block_id);
  });

  // Launch the rpc server to listen for requests
  server_->run(true, num_worker_threads);
}

DataServer::DataServer(u16 port, const std::string &data_path)
    : server_(std::make_unique<RpcServer>(port)) {
  initialize(data_path);
  // usize bitmap_blk_num = KDefaultBlockCnt / DiskBlockSize;
}

DataServer::DataServer(std::string const &address, u16 port,
                       const std::string &data_path)
    : server_(std::make_unique<RpcServer>(address, port)) {
  initialize(data_path);
}

DataServer::~DataServer() { server_.reset(); }

// {Your code here}
auto DataServer::read_data(block_id_t block_id, usize offset, usize len,
                           version_t version) -> std::vector<u8> {
  //UNIMPLEMENTED();
  auto new_version = this->get_blk_version(block_id);
  std::cout << "version: " << version << " new_version: " << new_version << std::endl;
  if (version != new_version) {
    return std::vector<u8>();
  }
  auto block_size = block_allocator_->bm->block_size();
  std::vector<u8> vec(block_size);
  block_allocator_->bm->read_block(block_id, vec.data());
  return std::vector<u8>(vec.begin() + offset, vec.begin() + offset + std::min(len, static_cast<usize>(vec.size() - offset)));
}

// {Your code here}
auto DataServer::write_data(block_id_t block_id, usize offset,
                            std::vector<u8> &buffer) -> bool {
  //UNIMPLEMENTED();
  if (buffer.size() > block_allocator_->bm->block_size() - offset)
    return false;
  block_allocator_->bm->write_partial_block(block_id, buffer.data(), offset, buffer.size());
  return true;
}

// {Your code here}
auto DataServer::alloc_block() -> std::pair<block_id_t, version_t> {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  auto blockid = block_allocator_->allocate();
  if (blockid.is_ok()) {
    auto new_version = this->update_blk_version(blockid.unwrap());
    // std::cout << "block id: " << blockid.unwrap() << "-- up_version: " << new_version << std::endl;
    return std::pair<block_id_t, version_t>(blockid.unwrap(), new_version);
  }

  return std::pair<block_id_t, version_t>(0, 0);
}

// {Your code here}
auto DataServer::free_block(block_id_t block_id) -> bool {
  // UNIMPLEMENTED();
  if (block_allocator_->deallocate(block_id).is_ok()) {
    // this->block_allocator_->bm->zero_block(block_id);
    this->update_blk_version(block_id);
    return true;
  }
  return false;
}

auto DataServer::get_blk_version(block_id_t block_id) -> version_t {
  auto version_perblk = DiskBlockSize / sizeof(version_t);
  usize blk_idx = block_id / version_perblk;
  usize idx = block_id % version_perblk;
  std::vector<u8> vec(DiskBlockSize);
  this->block_allocator_->bm->read_block(blk_idx, vec.data());
  return ((version_t *)(vec.data()))[idx];
}

auto DataServer::update_blk_version(block_id_t block_id) -> version_t {
  auto version_perblk = DiskBlockSize / sizeof(version_t);
  usize blk_idx = block_id / version_perblk;
  usize idx = block_id % version_perblk;
  std::vector<u8> vec(DiskBlockSize);
  this->block_allocator_->bm->read_block(blk_idx, vec.data());
  ((version_t *)(vec.data()))[idx] += 1;
  this->block_allocator_->bm->write_block(blk_idx, vec.data());
  return ((version_t *)(vec.data()))[idx];
}
} // namespace chfs
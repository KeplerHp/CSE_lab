#include "distributed/client.h"
#include "common/macros.h"
#include "common/util.h"
#include "distributed/metadata_server.h"

namespace chfs {

ChfsClient::ChfsClient() : num_data_servers(0) {}

auto ChfsClient::reg_server(ServerType type, const std::string &address,
                            u16 port, bool reliable) -> ChfsNullResult {
  switch (type) {
  case ServerType::DATA_SERVER:
    num_data_servers += 1;
    data_servers_.insert({num_data_servers, std::make_shared<RpcClient>(
                                                address, port, reliable)});
    break;
  case ServerType::METADATA_SERVER:
    metadata_server_ = std::make_shared<RpcClient>(address, port, reliable);
    break;
  default:
    std::cerr << "Unknown Type" << std::endl;
    exit(1);
  }

  return KNullOk;
}

// {Your code here}
auto ChfsClient::mknode(FileType type, inode_id_t parent,
                        const std::string &name) -> ChfsResult<inode_id_t> {

  auto res = this->metadata_server_->call("mknode", (u8)type, parent, name);
  if (res.is_err())
    return ChfsResult<inode_id_t>(ErrorType::BadResponse);
  auto res_inode_id = res.unwrap()->as<inode_id_t>();
  if (res_inode_id == 0) {
    return ChfsResult<inode_id_t>(ErrorType::INVALID);
  }
  return res_inode_id;
}

// {Your code here}
auto ChfsClient::unlink(inode_id_t parent, std::string const &name)
    -> ChfsNullResult {

  auto res = this->metadata_server_->call("unlink", parent, name);
  if (res.is_err())
    return ChfsNullResult(ErrorType::BadResponse);
  auto res_bool = res.unwrap()->as<bool>();
  if (res_bool == false)
    return ChfsNullResult(ErrorType::INVALID);
  return KNullOk;
}

// {Your code here}
auto ChfsClient::lookup(inode_id_t parent, const std::string &name)
    -> ChfsResult<inode_id_t> {

  auto res = this->metadata_server_->call("lookup", parent, name);
  std::cout << "res.un: " << res.unwrap() << std::endl;
  if (res.is_err())
    return ChfsResult<inode_id_t>(0);
  auto res_inode_id = res.unwrap()->as<inode_id_t>();
  if (res_inode_id == 0) {
    return ChfsResult<inode_id_t>(0);
  }
  return ChfsResult<inode_id_t>(res_inode_id);
}

// {Your code here}
auto ChfsClient::readdir(inode_id_t id)
    -> ChfsResult<std::vector<std::pair<std::string, inode_id_t>>> {

  auto res = this->metadata_server_->call("readdir", id);
  if (res.is_err())
    return ChfsResult<std::vector<std::pair<std::string, inode_id_t>>>({});
  auto res_vec = res.unwrap()->as<std::vector<std::pair<std::string, inode_id_t>>>();
  return res_vec;
}

// {Your code here}
auto ChfsClient::get_type_attr(inode_id_t id)
    -> ChfsResult<std::pair<InodeType, FileAttr>> {

  auto res = this->metadata_server_->call("get_type_attr", id);
  if (res.is_err())
    return ChfsResult<std::pair<InodeType, FileAttr>>({});
  auto [size, atime, mtime, ctime, type] = res.unwrap()->as<std::tuple<u64, u64, u64, u64, u8>>();
  FileAttr file_attr;
  file_attr.atime = atime;
  file_attr.mtime = mtime;
  file_attr.ctime = ctime;
  file_attr.size = size;
  return ChfsResult<std::pair<InodeType, FileAttr>>(std::pair<InodeType, FileAttr>(static_cast<InodeType>(type), file_attr));
}

/**
 * Read and Write operations are more complicated.
 */
// {Your code here}
auto ChfsClient::read_file(inode_id_t id, usize offset, usize size)
    -> ChfsResult<std::vector<u8>> {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  auto res = this->metadata_server_->call("get_block_map", id);
  if (res.is_err())
    return ChfsResult<std::vector<u8>>({});
  auto res_un = res.unwrap()->as<std::vector<BlockInfo>>();
  auto file_size = res_un.size() * DiskBlockSize;
  if (offset + size > file_size)
    return ChfsResult<std::vector<u8>>({});
  std::vector<u8> content;
  for (auto [blk_id, mac_id, version] : res_un) {
    auto read_res = this->data_servers_[mac_id]->call("read_data", blk_id, 0, DiskBlockSize, version);
    if (read_res.is_err()) {
      return ChfsResult<std::vector<u8>>({});
    }
    auto buffer = read_res.unwrap()->as<std::vector<u8>>();
    if (buffer.empty()) {
      return ChfsResult<std::vector<u8>>({});
    }
    content.insert(content.end(), buffer.begin(), buffer.end());
  }
  return ChfsResult<std::vector<u8>>(std::vector<u8>(content.begin() + offset, content.begin() + offset + size));
}

// {Your code here}
auto ChfsClient::write_file(inode_id_t id, usize offset, std::vector<u8> data)
    -> ChfsNullResult {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  auto res = this->metadata_server_->call("get_block_map", id);
  if (res.is_err())
    return ChfsNullResult(ErrorType::BadResponse);
  auto res_un = res.unwrap()->as<std::vector<BlockInfo>>();
  auto old_file_sz = res_un.size() * DiskBlockSize;
  auto old_blk_num = res_un.size();

  auto read_res = this->read_file(id, 0, old_file_sz);
  if (read_res.is_err())
    return ChfsNullResult(ErrorType::INVALID);
  auto read_res_vec = read_res.unwrap();
  if (offset + data.size() > read_res_vec.size()) {
    read_res_vec.resize(offset + data.size());
  }

  memcpy(read_res_vec.data() + offset, data.data(), data.size());
  usize new_blk_num = read_res_vec.size() / DiskBlockSize;
  if (read_res_vec.size() % DiskBlockSize > 0) 
    new_blk_num += 1;


  for (u32 i = 0; i < new_blk_num - old_blk_num; i++) {
    auto allocate_res = this->metadata_server_->call("alloc_block", id);
    if (allocate_res.is_err())
      return ChfsNullResult(ErrorType::BadResponse);
    auto res_info = allocate_res.unwrap()->as<BlockInfo>();
    if (std::get<0>(res_info) == 0 && std::get<1>(res_info) == 0 && std::get<2>(res_info) == 0)
      return ChfsNullResult(ErrorType::NotExist);
    res_un.emplace_back(res_info);
  }

  auto left_size = read_res_vec.size();
  u32 count = 0;
  for (auto [blk_id, mac_id, version] : res_un) {
    usize per_size = left_size >= DiskBlockSize ? DiskBlockSize : left_size; 
    auto write_res = this->data_servers_[mac_id]->call("write_data", blk_id, 0, std::vector<u8>(read_res_vec.begin() + DiskBlockSize * count, read_res_vec.begin() + DiskBlockSize * count + per_size));
    if (write_res.is_err() || write_res.unwrap()->as<bool>() == false) 
      return ChfsNullResult(ErrorType::INVALID);
    left_size -= per_size;
    count++;
  }

  return KNullOk;
}

// {Your code here}
auto ChfsClient::free_file_block(inode_id_t id, block_id_t block_id,
                                 mac_id_t mac_id) -> ChfsNullResult {
  auto res = this->metadata_server_->call("free_block", id, block_id, mac_id);
  if (res.is_err())
    return ChfsNullResult(ErrorType::BadResponse);
  auto res_res = res.unwrap()->as<bool>();
  if (res_res == false)
    return ChfsNullResult(ErrorType::INVALID);

  return KNullOk;
}

} // namespace chfs
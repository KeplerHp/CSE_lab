#include <algorithm>
#include <sstream>

#include "filesystem/directory_op.h"

namespace chfs {

/**
 * Some helper functions
 */
auto string_to_inode_id(std::string &data) -> inode_id_t {
  std::stringstream ss(data);
  inode_id_t inode;
  ss >> inode;
  return inode;
}

auto inode_id_to_string(inode_id_t id) -> std::string {
  std::stringstream ss;
  ss << id;
  return ss.str();
}

// {Your code here}
auto dir_list_to_string(const std::list<DirectoryEntry> &entries)
    -> std::string {
  std::ostringstream oss;
  usize cnt = 0;
  for (const auto &entry : entries) {
    oss << entry.name << ':' << entry.id;
    if (cnt < entries.size() - 1) {
      oss << '/';
    }
    cnt += 1;
  }
  return oss.str();
}

// {Your code here}
auto append_to_directory(std::string src, std::string filename, inode_id_t id)
    -> std::string {

  // TODO: Implement this function.
  //       Append the new directory entry to `src`.
  // UNIMPLEMENTED();
  // std::cout << "test filename: " << filename << std::endl;
  std::stringstream ss;
  if (src == "") {
    ss << filename << ":" << id;
  } else {
    ss << "/" << filename << ":" << id;
  }
  
  src.append(ss.str()); 
  
  return src;
}

// {Your code here}
void parse_directory(std::string &src, std::list<DirectoryEntry> &list) {

  // TODO: Implement this function.
  // UNIMPLEMENTED();

  // std::cout << "src in func parse_dir" << src << std::endl;

  size_t pos = 0;
  while(pos != std::string::npos) {
    size_t colon_pos = src.find(":", pos);
    size_t slash_pos = src.find("/", colon_pos);

    if (colon_pos != std::string::npos) {
      std::string name = src.substr(pos, colon_pos - pos);
      std::string inode;

      if (slash_pos != std::string::npos) {
        inode = src.substr(colon_pos + 1, slash_pos - colon_pos - 1);
        pos = slash_pos + 1;
      } else {
        inode = src.substr(colon_pos + 1);
        pos = std::string::npos;
      }

      inode_id_t inode_id = string_to_inode_id(inode);//std::stoull(inode);
      DirectoryEntry dir;
      dir.name = name;
      dir.id = inode_id;
      list.emplace_back(dir);
    } else {
      pos = std::string::npos;
    }
  }

}

// {Your code here}
auto rm_from_directory(std::string src, std::string filename) -> std::string {

  auto res = std::string("");

  // TODO: Implement this function.
  //       Remove the directory entry from `src`.
  // UNIMPLEMENTED();

  std::list<DirectoryEntry> dir_list;
  parse_directory(src, dir_list);
  for (auto i = dir_list.begin(); i != dir_list.end(); i++) {
    // std::cout << "filename: " << filename << " " << "i.name: " << i->name << std::endl;
    if (i->name == filename) {
      dir_list.erase(i);
      break;
    }
  }
  res = dir_list_to_string(dir_list);

  return res;
}

/**
 * { Your implementation here }
 */
auto read_directory(FileOperation *fs, inode_id_t id,
                    std::list<DirectoryEntry> &list) -> ChfsNullResult {
  
  // TODO: Implement this function.
  // UNIMPLEMENTED();

  auto res = fs->read_file(id);

  //std::cout << src << std::endl;
  
  std::vector<u8> res_data;
  if(res.is_ok()) {
    res_data = res.unwrap();
    // if(res_data.size() != 0) {
    //   std::cout << res_data[0] << std::endl;
    // }
  } else {
    return ChfsNullResult(ErrorType::INVALID);
  }

  std::string src(res_data.begin(), res_data.end());

  // std::cout << src << std::endl;

  parse_directory(src, list);

  return KNullOk;
}

// {Your code here}
auto FileOperation::lookup(inode_id_t id, const char *name)
    -> ChfsResult<inode_id_t> {
  std::list<DirectoryEntry> list;

  // TODO: Implement this function.
  // UNIMPLEMENTED();
  read_directory(this, id, list);
  for (auto i : list) {
    //std::cout << i.name << std::endl;
    if (i.name == name) {
      return ChfsResult<inode_id_t>(i.id);
    }
  }

  return ChfsResult<inode_id_t>(ErrorType::NotExist);
}

// {Your code here}
auto FileOperation::mk_helper(inode_id_t id, const char *name, InodeType type)
    -> ChfsResult<inode_id_t> {

  // TODO:
  // 1. Check if `name` already exists in the parent.
  //    If already exist, return ErrorType::AlreadyExist.
  // 2. Create the new inode.
  // 3. Append the new entry to the parent directory.
  // UNIMPLEMENTED();
  std::list<DirectoryEntry> list;
  read_directory(this, id, list);
  for (auto i : list) {
    if(i.name == name) {
      std::cout << "already exist" << std::endl;
      return ChfsResult<inode_id_t>(ErrorType::AlreadyExist);
    }
  }
  auto inode_id_res = alloc_inode(type);
  if (inode_id_res.is_err()) 
  {
    std::cout << "inode wrong" << std::endl;
    return ChfsResult<inode_id_t>(ErrorType::NotExist);
  }
  auto inode_id = inode_id_res.unwrap();
  // std::cout << "inode id : " << inode_id << std::endl;

  std::string str(dir_list_to_string(list));
  str = append_to_directory(str, name, inode_id);
  // std::cerr << name << std::endl;
  // std::cout << "str:" << str << std::endl;
  std::vector<u8> vec(str.begin(), str.end());
  write_file(id, vec);

  return ChfsResult<inode_id_t>(inode_id);
}

// {Your code here}
auto FileOperation::unlink(inode_id_t parent, const char *name)
    -> ChfsNullResult {

  // TODO: 
  // 1. Remove the file, you can use the function `remove_file`
  // 2. Remove the entry from the directory.
  // UNIMPLEMENTED();

  auto res = lookup(parent, name);
  
  inode_id_t inode_id = 0;
  if (res.is_ok()) {
    inode_id = res.unwrap();
  }
  remove_file(inode_id);

  // std::list<DirectoryEntry> list;
  auto res1 = read_file(parent);

  std::vector<u8> res_data;
  if(res1.is_ok()) {
    res_data = res1.unwrap();
  } else {
    return ChfsNullResult(ErrorType::INVALID);
  }

  std::string src(res_data.begin(), res_data.end());

  src = rm_from_directory(src, name);

  std::vector<u8> vec(src.begin(), src.end());

  write_file(parent, vec);

  //inode_p->inner_attr.size = 
  
  return KNullOk;
}

} // namespace chfs

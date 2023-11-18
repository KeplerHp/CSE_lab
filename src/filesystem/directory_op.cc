#include <algorithm>
#include <sstream>

#include "filesystem/directory_op.h"

namespace chfs
{

  /**
   * Some helper functions
   */
  auto string_to_inode_id(std::string &data) -> inode_id_t
  {
    std::stringstream ss(data);
    inode_id_t inode;
    ss >> inode;
    return inode;
  }

  auto inode_id_to_string(inode_id_t id) -> std::string
  {
    std::stringstream ss;
    ss << id;
    return ss.str();
  }

  // {Your code here}
  auto dir_list_to_string(const std::list<DirectoryEntry> &entries)
      -> std::string
  {
    std::ostringstream oss;
    usize cnt = 0;
    for (const auto &entry : entries)
    {
      oss << entry.name << ':' << entry.id;
      if (cnt < entries.size() - 1)
      {
        oss << '/';
      }
      cnt += 1;
    }
    return oss.str();
  }

  // {Your code here}
  auto append_to_directory(std::string src, std::string filename, inode_id_t id)
      -> std::string
  {

    // TODO: Implement this function.
    //       Append the new directory entry to `src`.
    std::list<DirectoryEntry> list;
    parse_directory(src, list);
    DirectoryEntry de;
    de.name = filename;
    de.id = id;
    list.push_back(de);
    src = dir_list_to_string(list);
    return src;
  }

  // {Your code here}
  void parse_directory(std::string &src, std::list<DirectoryEntry> &list)
  {

    // TODO: Implement this function.
    // UNIMPLEMENTED();
    std::string remaining = src;

    while (!remaining.empty())
    {
      size_t entryEndPos = remaining.find('/');
      std::string entry = remaining.substr(0, entryEndPos);
      size_t idStartPos = entry.find(':');
      std::string name = entry.substr(0, idStartPos);
      std::string id_str = entry.substr(idStartPos + 1);
      auto id = string_to_inode_id(id_str);

      DirectoryEntry de;
      de.name = name;
      de.id = id;
      list.push_back(de);

      if (entryEndPos == std::string::npos)
      {
        break;
      }
      remaining = remaining.substr(entryEndPos + 1);
    }
  }

  // {Your code here}
  auto rm_from_directory(std::string src, std::string filename) -> std::string
  {

    auto res = std::string("");

    // TODO: Implement this function.
    //       Remove the directory entry from `src`.
    // UNIMPLEMENTED();
    std::list<DirectoryEntry> list;
    parse_directory(src, list);
    for (auto it = list.begin(); it != list.end(); it++)
    {
      if (it->name == filename)
      {
        list.erase(it);
        break;
      }
    }
    res = dir_list_to_string(list);

    return res;
  }

  /**
   * { Your implementation here }
   */
  auto read_directory(FileOperation *fs, inode_id_t id,
                      std::list<DirectoryEntry> &list) -> ChfsNullResult
  {

    // TODO: Implement this function.
    // UNIMPLEMENTED();
    auto read_res = fs->read_file(id);
    if (read_res.is_err())
    {
      return ChfsNullResult(read_res.unwrap_error());
    }
    auto data = read_res.unwrap();

    std::string src(data.begin(), data.end());
    parse_directory(src, list);
    return KNullOk;
  }

  // {Your code here}
  auto FileOperation::lookup(inode_id_t id, const char *name)
      -> ChfsResult<inode_id_t>
  {
    std::list<DirectoryEntry> list;

    // TODO: Implement this function.
    // UNIMPLEMENTED();
    auto read_res = read_directory(this, id, list);
    if (read_res.is_err())
    {
      return ChfsResult<inode_id_t>(read_res.unwrap_error());
    }

    for (const auto &entry : list)
    {
      if (entry.name.compare(name) == 0)
      {
        return ChfsResult<inode_id_t>(entry.id);
      }
    }

    return ChfsResult<inode_id_t>(ErrorType::NotExist);
  }

  // {Your code here}
  auto FileOperation::mk_helper(inode_id_t id, const char *name, InodeType type)
      -> ChfsResult<inode_id_t>
  {

    // TODO:
    // 1. Check if `name` already exists in the parent.
    //    If already exist, return ErrorType::AlreadyExist.
    // 2. Create the new inode.
    // 3. Append the new entry to the parent directory.
    // UNIMPLEMENTED();
    std::list<DirectoryEntry> list;
    auto read_res = read_directory(this, id, list);
    if (read_res.is_err())
    {
      return ChfsResult<inode_id_t>(read_res.unwrap_error());
    }
    for (const auto &entry : list)
    {
      if (entry.name == name)
      {
        return ChfsResult<inode_id_t>(ErrorType::AlreadyExist);
      }
    }

    auto alloc_res = alloc_inode(type);
    if (alloc_res.is_err())
    {
      return ChfsResult<inode_id_t>(alloc_res.unwrap_error());
    }

    auto new_id = alloc_res.unwrap();
    DirectoryEntry de;
    de.name = name;
    de.id = new_id;
    list.push_back(de);
    std::string content = dir_list_to_string(list);
    std::vector<u8> data(content.begin(), content.end());
    auto write_res = write_file(id, data);

    if (write_res.is_err())
    {
      return ChfsResult<inode_id_t>(write_res.unwrap_error());
    }
    return ChfsResult<inode_id_t>(new_id);
  }

  // {Your code here}
  auto FileOperation::unlink(inode_id_t parent, const char *name)
      -> ChfsNullResult
  {

    // TODO:
    // 1. Remove the file, you can use the function `remove_file`
    // 2. Remove the entry from the directory.
    // UNIMPLEMENTED();
    std::list<DirectoryEntry> list;
    auto read_res = read_directory(this, parent, list);
    if (read_res.is_err())
    {
      return ChfsNullResult(read_res.unwrap_error());
    }
    for (auto it = list.begin(); it != list.end(); it++)
    {
      if (it->name.compare(name) == 0)
      {
        auto remove_res = remove_file(it->id);
        if (remove_res.is_err())
        {
          return ChfsNullResult(remove_res.unwrap_error());
        }
        list.erase(it);
        break;
      }
    }
    // write back to parent
    std::string content = dir_list_to_string(list);
    std::vector<u8> data(content.begin(), content.end());
    auto write_res = write_file(parent, data);
    if (write_res.is_err())
    {
      return ChfsNullResult(write_res.unwrap_error());
    }

    return KNullOk;
  }

} // namespace chfs

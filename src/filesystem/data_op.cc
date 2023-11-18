#include <ctime>

#include "filesystem/operations.h"

namespace chfs
{

  // {Your code here}
  auto FileOperation::alloc_inode(InodeType type) -> ChfsResult<inode_id_t>
  {
    inode_id_t inode_id = static_cast<inode_id_t>(0);
    auto inode_res = ChfsResult<inode_id_t>(inode_id);

    // TODO:
    // 1. Allocate a block for the inode.
    // 2. Allocate an inode.
    // 3. Initialize the inode block
    //    and write the block back to block manager.
    auto block_id_res = this->block_allocator_->allocate();
    if (block_id_res.is_err())
    {
      return ChfsResult<inode_id_t>(block_id_res.unwrap_error());
    }
    auto block_id = block_id_res.unwrap();

    auto inode_id_res = this->inode_manager_->allocate_inode(type, block_id);
    if (inode_id_res.is_err())
    {
      return ChfsResult<inode_id_t>(inode_id_res.unwrap_error());
    }
    inode_id = inode_id_res.unwrap();

    inode_res = ChfsResult<inode_id_t>(inode_id);
    return inode_res;
  }

  auto FileOperation::getattr(inode_id_t id) -> ChfsResult<FileAttr>
  {
    return this->inode_manager_->get_attr(id);
  }

  auto FileOperation::get_type_attr(inode_id_t id)
      -> ChfsResult<std::pair<InodeType, FileAttr> >
  {
    return this->inode_manager_->get_type_attr(id);
  }

  auto FileOperation::gettype(inode_id_t id) -> ChfsResult<InodeType>
  {
    return this->inode_manager_->get_type(id);
  }

  auto calculate_block_sz(u64 file_sz, u64 block_sz) -> u64
  {
    return (file_sz % block_sz) ? (file_sz / block_sz + 1) : (file_sz / block_sz);
  }

  auto FileOperation::write_file_w_off(inode_id_t id, const char *data, u64 sz,
                                       u64 offset) -> ChfsResult<u64>
  {
    auto read_res = this->read_file(id);
    if (read_res.is_err())
    {
      return ChfsResult<u64>(read_res.unwrap_error());
    }

    auto content = read_res.unwrap();
    if (offset + sz > content.size())
    {
      content.resize(offset + sz);
    }
    memcpy(content.data() + offset, data, sz);

    auto write_res = this->write_file(id, content);
    if (write_res.is_err())
    {
      return ChfsResult<u64>(write_res.unwrap_error());
    }
    return ChfsResult<u64>(sz);
  }

  // {Your code here}
  auto FileOperation::write_file(inode_id_t id, const std::vector<u8> &content)
      -> ChfsNullResult
  {

    auto error_code = ErrorType::DONE;
    const auto block_size = this->block_manager_->block_size();
    usize old_block_num = 0;
    usize new_block_num = 0;
    u64 original_file_sz = 0;

    // 1. read the inode
    std::vector<u8> inode(block_size);
    std::vector<u8> indirect_block(0);
    indirect_block.reserve(block_size);

    auto inlined_blocks_num = 0;

    auto inode_res = this->inode_manager_->read_inode(id, inode);

    auto inode_p = reinterpret_cast<Inode *>(inode.data());
    if (inode_res.is_err())
    {
      error_code = inode_res.unwrap_error();
      // I know goto is bad, but we have no choice
      goto err_ret;
    }
    else
    {
      inlined_blocks_num = inode_p->get_direct_block_num();
      std::cout << inlined_blocks_num << std::endl;
    }
    std::cout << "filesystem/data_op/write_file: content.size()=" << content.size() << std::endl;
    if (content.size() > inode_p->max_file_sz_supported())
    {
      std::cerr << "filesystem/data_op: file size too large: " << content.size() << " vs. "
                << inode_p->max_file_sz_supported() << std::endl;
      error_code = ErrorType::OUT_OF_RESOURCE;
      goto err_ret;
    }

    // 2. make sure whether we need to allocate more blocks
    original_file_sz = inode_p->get_size();
    old_block_num = calculate_block_sz(original_file_sz, block_size);
    new_block_num = calculate_block_sz(content.size(), block_size);

    if (old_block_num > inlined_blocks_num)
    {
      auto indirect_block_id = inode_p->get_indirect_block_id();
      indirect_block.resize(block_size);
      auto read_res = this->block_manager_->read_block(indirect_block_id, indirect_block.data());
      if (read_res.is_err())
      {
        error_code = read_res.unwrap_error();
        goto err_ret;
      }
    }
    if (new_block_num > old_block_num)
    {
      // If we need to allocate more blocks.
      for (usize idx = old_block_num; idx < new_block_num; ++idx)
      {
        // TODO: Implement the case of allocating more blocks.
        // 1. Allocate a block.
        // 2. Fill the allocated block id to the inode.
        //    You should pay attention to the case of indirect block.
        //    You may use function `get_or_insert_indirect_block`
        //    in the case of indirect block.
        auto alloc_res = block_allocator_->allocate();
        if (alloc_res.is_err())
        {
          error_code = alloc_res.unwrap_error();
          goto err_ret;
        }
        auto block_id = alloc_res.unwrap();
        if (inode_p->is_direct_block(idx))
        {

          inode_p->blocks[idx] = block_id;
        }
        else
        {
          // is indirect block
          if (inode_p->blocks[inode_p->nblocks - 1] == KInvalidBlockID)
          {
            // first indirect block, allocate the indirect block

            auto extra_alloc_res = block_allocator_->allocate();
            if (extra_alloc_res.is_err())
            {
              error_code = alloc_res.unwrap_error();
              goto err_ret;
            }
            auto last_block_id = extra_alloc_res.unwrap();
            inode_p->blocks[idx] = last_block_id;
          }

          // add block_id_t(u64) to indirect block(u8*)
          for (auto i = 0; i < 8; i++)
            indirect_block[8 * (idx - inlined_blocks_num) + i] = (u8)(block_id >> (i * 8));
        }
      }
      // write back inode
      auto inode_block_id_res = this->inode_manager_->get(id);
      if (inode_block_id_res.is_err())
      {
        error_code = inode_block_id_res.unwrap_error();
        goto err_ret;
      }
      auto inode_block_id = inode_block_id_res.unwrap();
      auto write_res = this->block_manager_->write_block(inode_block_id, inode.data());
      if (write_res.is_err())
      {
        error_code = write_res.unwrap_error();
        goto err_ret;
      }
      // write back indirect block if exists
      if (inode_p->blocks[inode_p->nblocks - 1] != KInvalidBlockID)
      {

        write_res = this->block_manager_->write_block(inode_p->get_indirect_block_id(), indirect_block.data());
        if (write_res.is_err())
        {
          error_code = write_res.unwrap_error();
          goto err_ret;
        }
      }
    }
    else
    {
      // We need to free the extra blocks.
      for (usize idx = new_block_num; idx < old_block_num; ++idx)
      {
        if (inode_p->is_direct_block(idx))
        {
          // TODO: Free the direct extra block.
          block_allocator_->deallocate(inode_p->blocks[idx]);
        }
        else
        {
          // TODO: Free the indirect extra block.
          auto indirect_block_id = inode_p->get_indirect_block_id();
          auto indirect_block_res = this->block_manager_->read_block(indirect_block_id, indirect_block.data());
          if (indirect_block_res.is_err())
          {
            error_code = indirect_block_res.unwrap_error();
            goto err_ret;
          }
          block_id_t tmp_block_id = 0;
          for (auto i = 0; i < 8; i++)
            tmp_block_id += (u64)indirect_block[8 * (idx - inlined_blocks_num) + i] << (i * 8);
          block_allocator_->deallocate(tmp_block_id);
        }
      }
      // If there are no more indirect blocks.
      if (old_block_num > inlined_blocks_num &&
          new_block_num <= inlined_blocks_num && true)
      {

        auto res =
            this->block_allocator_->deallocate(inode_p->get_indirect_block_id());
        if (res.is_err())
        {
          error_code = res.unwrap_error();
          goto err_ret;
        }
        indirect_block.clear();
        inode_p->invalid_indirect_block_id();
      }
    }

    // 3. write the contents
    inode_p->inner_attr.size = content.size();
    inode_p->inner_attr.mtime = time(0);
    {
      auto block_idx = 0;
      u64 write_sz = 0;
      if (inode_p->blocks[inode_p->nblocks - 1] != KInvalidBlockID)
      {
        auto indirect_block_id = inode_p->get_indirect_block_id();
        auto indirect_block_res = this->block_manager_->read_block(indirect_block_id, indirect_block.data());
        if (indirect_block_res.is_err())
        {
          error_code = indirect_block_res.unwrap_error();
          goto err_ret;
        }
      }
      while (write_sz < content.size())
      {
        auto sz = ((content.size() - write_sz) > block_size)
                      ? block_size
                      : (content.size() - write_sz);
        std::vector<u8> buffer(block_size);
        memcpy(buffer.data(), content.data() + write_sz, sz);
        block_id_t curr_block_id = 0;
        if (inode_p->is_direct_block(block_idx))
        {
          // TODO: Implement getting block id of current direct block.
          curr_block_id = inode_p->blocks[block_idx];
        }
        else
        {
          // TODO: Implement getting block id of current indirect block.
          block_id_t tmp_block_id = 0;
          for (auto i = 0; i < 8; i++)
            tmp_block_id += (u64)indirect_block[8 * (block_idx - inlined_blocks_num) + i] << (i * 8);
          curr_block_id = tmp_block_id;
        }
        // TODO: Write to current block.
        auto write_res = this->block_manager_->write_block(curr_block_id, buffer.data());
        if (write_res.is_err())
        {
          error_code = write_res.unwrap_error();
          goto err_ret;
        }

        write_sz += sz;
        block_idx += 1;
      }
    }

    // finally, update the inode
    {
      inode_p->inner_attr.set_all_time(time(0));

      auto write_res =
          this->block_manager_->write_block(inode_res.unwrap(), inode.data());
      if (write_res.is_err())
      {
        error_code = write_res.unwrap_error();
        goto err_ret;
      }
      if (indirect_block.size() != 0)
      {
        write_res =
            inode_p->write_indirect_block(this->block_manager_, indirect_block);
        if (write_res.is_err())
        {
          error_code = write_res.unwrap_error();
          goto err_ret;
        }
      }
    }

    return KNullOk;

  err_ret:
    // std::cerr << "write file return error: " << (int)error_code << std::endl;
    return ChfsNullResult(error_code);
  }

  // {Your code here}
  auto FileOperation::read_file(inode_id_t id) -> ChfsResult<std::vector<u8> >
  {
    auto error_code = ErrorType::DONE;
    std::vector<u8> content;

    const auto block_size = this->block_manager_->block_size();

    // 1. read the inode
    std::vector<u8> inode(block_size);
    std::vector<u8> indirect_block(0);
    indirect_block.reserve(block_size);
    auto inode_p = reinterpret_cast<Inode *>(inode.data());
    u64 file_sz = 0;
    u64 read_sz = 0;
    u64 inline_blocks_num = 0;

    auto inode_res = this->inode_manager_->read_inode(id, inode);
    if (inode_res.is_err())
    {
      error_code = inode_res.unwrap_error();
      // I know goto is bad, but we have no choice
      goto err_ret;
    }

    file_sz = inode_p->get_size();
    inline_blocks_num = inode_p->get_direct_block_num();
    content.reserve(file_sz);

    // Now read the file
    while (read_sz < file_sz)
    {
      auto sz = ((inode_p->get_size() - read_sz) > block_size)
                    ? block_size
                    : (inode_p->get_size() - read_sz);
      std::vector<u8> buffer(block_size);
      block_id_t curr_block_id = 0;
      auto idx = read_sz / block_size;
      // Get current block id
      if (inode_p->is_direct_block(idx))
      {
        // TODO: Implement the case of direct block.
        curr_block_id = inode_p->blocks[idx];
      }
      else
      {

        // TODO: Implement the case of indirect block.
        if (idx == inline_blocks_num)
        {
          auto read_indirect_res = this->block_manager_->read_block(inode_p->get_indirect_block_id(), indirect_block.data());
          if (read_indirect_res.is_err())
          {
            error_code = read_indirect_res.unwrap_error();
            goto err_ret;
          }
        }
        block_id_t tmp_block_id = 0;
        for (auto i = 0; i < 8; i++)
          tmp_block_id += (block_id_t)indirect_block[8 * (idx - inline_blocks_num) + i] << (i * 8);
        curr_block_id = tmp_block_id;
      }
      auto read_res = this->block_manager_->read_block(curr_block_id, buffer.data());
      if (read_res.is_err())
      {
        error_code = read_res.unwrap_error();
        goto err_ret;
      }
      content.insert(content.end(), buffer.begin(), (buffer.begin() + sz > buffer.end() + 8 ? buffer.end() + 8 : buffer.begin() + sz));
      read_sz += sz;
    }
    return ChfsResult<std::vector<u8> >(std::move(content));
  err_ret:
    return ChfsResult<std::vector<u8> >(error_code);
  }

  auto FileOperation::read_file_w_off(inode_id_t id, u64 sz, u64 offset)
      -> ChfsResult<std::vector<u8> >
  {
    auto res = read_file(id);
    if (res.is_err())
    {
      return res;
    }

    auto content = res.unwrap();
    return ChfsResult<std::vector<u8> >(
        std::vector<u8>(content.begin() + offset, content.begin() + offset + sz));
  }

  auto FileOperation::resize(inode_id_t id, u64 sz) -> ChfsResult<FileAttr>
  {
    auto attr_res = this->getattr(id);
    if (attr_res.is_err())
    {
      return ChfsResult<FileAttr>(attr_res.unwrap_error());
    }

    auto attr = attr_res.unwrap();
    auto file_content = this->read_file(id);
    if (file_content.is_err())
    {
      return ChfsResult<FileAttr>(file_content.unwrap_error());
    }

    auto content = file_content.unwrap();

    if (content.size() != sz)
    {
      content.resize(sz);

      auto write_res = this->write_file(id, content);
      if (write_res.is_err())
      {
        return ChfsResult<FileAttr>(write_res.unwrap_error());
      }
    }

    attr.size = sz;
    return ChfsResult<FileAttr>(attr);
  }

} // namespace chfs
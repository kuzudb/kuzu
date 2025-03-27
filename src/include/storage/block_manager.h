#pragma once

#include "common/types/types.h"
#include "storage/file_handle.h"
namespace kuzu::storage {
class FreeSpaceManager;

struct BlockEntry {
    common::page_idx_t startPageIdx;
    common::page_idx_t numPages;
};

class BlockManager {
public:
    BlockManager();

    BlockEntry allocateBlock(common::page_idx_t numPages);
    void freeBlock(BlockEntry block);

private:
    std::unique_ptr<FreeSpaceManager> freeSpaceManager;
    FileHandle* handle;
};
} // namespace kuzu::storage

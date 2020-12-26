#pragma once

#include "src/common/include/configs.h"
#include "src/storage/include/buffer_manager.h"

using namespace graphflow::common;

namespace graphflow {
namespace storage {

class BaseColumnOrList {

protected:
    BaseColumnOrList(const string& fname, const size_t& elementSize, BufferManager& bufferManager)
        : elementSize{elementSize}, numElementsPerPage{(uint32_t)(PAGE_SIZE / elementSize)},
          fileHandle{fname}, bufferManager(bufferManager){};

protected:
    size_t elementSize;
    uint32_t numElementsPerPage;
    FileHandle fileHandle;
    BufferManager& bufferManager;
};

struct VectorFrameHandle {
    uint32_t pageIdx;
    bool isFrameBound;
    VectorFrameHandle() : pageIdx(-1), isFrameBound(false){};
};

} // namespace storage
} // namespace graphflow

#pragma once

#include "src/common/include/configs.h"
#include "src/storage/include/buffer_manager.h"

using namespace graphflow::common;

namespace graphflow {
namespace storage {

struct VectorFrameHandle {
    uint32_t pageIdx;
    bool isFrameBound;
    VectorFrameHandle() : pageIdx(-1), isFrameBound(false){};
};

class BaseColumnOrList {

public:
    void reclaim(unique_ptr<VectorFrameHandle>& handle);

protected:
    BaseColumnOrList(const string& fname, const size_t& elementSize, BufferManager& bufferManager);

    inline uint64_t getPageIdx(const uint64_t& offset) const {
        return offset / numElementsPerPage;
    }

    inline uint32_t getPageOffset(const uint64_t& nodeOffset) const {
        return (nodeOffset % numElementsPerPage) * elementSize;
    }

protected:
    shared_ptr<spdlog::logger> logger;
    size_t elementSize;
    uint32_t numElementsPerPage;
    FileHandle fileHandle;
    BufferManager& bufferManager;
};

} // namespace storage
} // namespace graphflow

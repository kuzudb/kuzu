#include "src/storage/include/structures/common.h"

namespace graphflow {
namespace storage {

BaseColumnOrList::BaseColumnOrList(
    const string& fname, const size_t& elementSize, BufferManager& bufferManager)
    : logger{spdlog::get("storage")}, elementSize{elementSize},
      numElementsPerPage{(uint32_t)(PAGE_SIZE / elementSize)}, fileHandle{fname},
      bufferManager(bufferManager){};

void BaseColumnOrList::reclaim(unique_ptr<VectorFrameHandle>& handle) {
    if (handle->isFrameBound) {
        bufferManager.unpin(fileHandle, handle->pageIdx);
    }
}

} // namespace storage
} // namespace graphflow

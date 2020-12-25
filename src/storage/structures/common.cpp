#include "src/storage/include/structures/common.h"

namespace graphflow {
namespace storage {

void BaseColumnOrList::reclaim(unique_ptr<VectorFrameHandle>& handle) {
    if (handle->isFrameBound) {
        bufferManager.unpin(fileHandle, handle->pageIdx);
    }
}

} // namespace storage
} // namespace graphflow

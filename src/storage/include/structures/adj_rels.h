#pragma once

#include "src/storage/include/structures/column.h"

namespace graphflow {
namespace storage {

class AdjRels : public BaseColumn {

public:
    AdjRels(const string path, uint64_t numElements, uint32_t numBytesPerLabel,
        uint32_t numBytesPerOffset, BufferManager& bufferManager)
        : BaseColumn(path, numBytesPerLabel + numBytesPerOffset, numElements, bufferManager),
          numBytesPerLabel{numBytesPerLabel}, numBytesPerOffset{numBytesPerOffset} {};

    inline void getVal(gfNodeOffset_t nodeOffset, gfLabel_t& label, gfNodeOffset_t& offset) {
        auto pageIdx = getPageIdx(nodeOffset, numElementsPerPage);
        auto frame = bufferManager.pin(fileHandle, pageIdx);
        auto pageOffset = frame + getPageOffset(nodeOffset, numElementsPerPage,
                                      numBytesPerLabel + numBytesPerOffset);
        memcpy(&label, (void*)pageOffset, numBytesPerLabel);
        memcpy(&offset, (void*)(pageOffset + numBytesPerLabel), numBytesPerOffset);
        bufferManager.unpin(fileHandle, pageIdx);
    }

private:
    uint32_t numBytesPerLabel;
    uint32_t numBytesPerOffset;
};

} // namespace storage
} // namespace graphflow

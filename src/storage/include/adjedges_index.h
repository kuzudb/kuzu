#pragma once

#include "src/storage/include/column.h"

namespace graphflow {
namespace storage {

typedef BaseColumn AdjEdgesBase;

class AdjEdges : public AdjEdgesBase {

public:
    AdjEdges(const string path, uint64_t numElements, uint32_t numBytesPerLabel,
        uint32_t numBytesPerOffset, BufferManager& bufferManager)
        : AdjEdgesBase(path, numBytesPerLabel + numBytesPerOffset, numElements, bufferManager),
          numBytesPerLabel{numBytesPerLabel}, numBytesPerOffset{numBytesPerOffset} {};

    inline static string getAdjEdgesIndexFname(
        const string& directory, gfLabel_t relLabel, gfLabel_t nodeLabel, Direction direction) {
        return directory + "/e-" + to_string(relLabel) + "-" + to_string(nodeLabel) + "-" +
               to_string(direction) + ".vcol";
    }

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

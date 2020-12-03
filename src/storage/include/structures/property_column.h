#pragma once

#include <string.h>

#include <bits/unique_ptr.h>

#include "src/storage/include/structures/column.h"

namespace graphflow {
namespace storage {

template<typename T>
class PropertyColumn : public ColumnBase {

public:
    PropertyColumn(const string path, uint64_t numElements, BufferManager& bufferManager)
        : ColumnBase{path, sizeof(T), numElements, bufferManager} {};

    inline void getVal(gfNodeOffset_t nodeOffset, T& t) {
        auto pageIdx = getPageIdx(nodeOffset, numElementsPerPage);
        auto frame = bufferManager.pin(fileHandle, pageIdx);
        memcpy(&t, (void*)(frame + getPageOffset(nodeOffset, numElementsPerPage, sizeof(T))),
            sizeof(T));
        bufferManager.unpin(fileHandle, pageIdx);
    }
};

typedef PropertyColumn<gfInt_t> PropertyColumnInteger;
typedef PropertyColumn<gfDouble_t> PropertyColumnDouble;
typedef PropertyColumn<gfBool_t> PropertyColumnBoolean;

} // namespace storage
} // namespace graphflow

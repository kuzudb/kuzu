#pragma once

#include <string.h>

#include <bits/unique_ptr.h>

#include "src/common/include/configs.h"
#include "src/common/include/types.h"
#include "src/storage/include/buffer_manager.h"
#include "src/storage/include/file_handle.h"

using namespace graphflow::common;
using namespace std;

namespace graphflow {
namespace storage {

class BaseColumn {

public:
    BaseColumn(
        const string fname, size_t elementSize, uint64_t numElements, BufferManager& bufferManager)
        : numElementsPerPage{PAGE_SIZE / elementSize},
          fileHandle{fname, 1 + (numElements / numElementsPerPage)}, bufferManager{bufferManager} {}

    inline static uint64_t getPageIdx(node_offset_t nodeOffset, uint32_t numElementsPerPage) {
        return nodeOffset / numElementsPerPage;
    }

    inline static uint32_t getPageOffset(
        node_offset_t nodeOffset, uint32_t numElementsPerPage, size_t elementSize) {
        return (nodeOffset % numElementsPerPage) * elementSize;
    }

protected:
    uint32_t numElementsPerPage;
    FileHandle fileHandle;
    BufferManager& bufferManager;
};

template<typename T>
class PropertyColumn : public BaseColumn {

public:
    PropertyColumn(const string path, uint64_t numElements, BufferManager& bufferManager)
        : BaseColumn{path, sizeof(T), numElements, bufferManager} {};

    inline void getVal(node_offset_t nodeOffset, T& t) {
        auto pageIdx = getPageIdx(nodeOffset, numElementsPerPage);
        auto frame = bufferManager.pin(fileHandle, pageIdx);
        memcpy(&t, (void*)(frame + getPageOffset(nodeOffset, numElementsPerPage, sizeof(T))),
            sizeof(T));
        bufferManager.unpin(fileHandle, pageIdx);
    }
};

typedef PropertyColumn<int32_t> PropertyColumnInt;
typedef PropertyColumn<double_t> PropertyColumnDouble;
typedef PropertyColumn<uint8_t> PropertyColumnBool;

} // namespace storage
} // namespace graphflow

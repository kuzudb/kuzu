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

class ColumnBase {

public:
    ColumnBase(
        const string path, size_t elementSize, uint64_t numElements, BufferManager &bufferManager)
        : numElementsPerPage(PAGE_SIZE / elementSize),
          fileHandle(FileHandle(path, 1 + (numElements / numElementsPerPage))),
          bufferManager(bufferManager) {}

    inline static uint64_t getPageIdx(gfNodeOffset_t nodeOffset, uint32_t numElementsPerPage) {
        return nodeOffset / numElementsPerPage;
    }

    inline static uint32_t getPageOffset(
        gfNodeOffset_t nodeOffset, uint32_t numElementsPerPage, size_t elementSize) {
        return (nodeOffset % numElementsPerPage) * elementSize;
    }

protected:
    uint32_t numElementsPerPage;
    FileHandle fileHandle;
    BufferManager &bufferManager;
};

template<typename T>
class Column : public ColumnBase {

public:
    Column(const string propertyName, const string path, uint64_t numElements,
        BufferManager &bufferManager)
        : ColumnBase(path, sizeof(T), numElements, bufferManager), propertyName(propertyName){};

    inline void getVal(gfNodeOffset_t nodeOffset, T &t) {
        auto pageIdx = getPageIdx(nodeOffset, numElementsPerPage);
        auto frame = bufferManager.pin(fileHandle, pageIdx);
        memcpy(&t, (void *)(frame + getPageOffset(nodeOffset, numElementsPerPage, sizeof(T))),
            sizeof(T));
        bufferManager.unpin(fileHandle, pageIdx);
    }

    inline const string &getPropertyName() { return propertyName; };

private:
    const string propertyName;
};

typedef Column<gfInt_t> ColumnInteger;
typedef Column<gfDouble_t> ColumnDouble;
typedef Column<gfBool_t> ColumnBoolean;
typedef Column<gfString_t> ColumnString;

} // namespace storage
} // namespace graphflow

#ifndef GRAPHFLOW_STORAGE_COLUMN_H_
#define GRAPHFLOW_STORAGE_COLUMN_H_

#include <bits/unique_ptr.h>

#include "src/common/include/configs.h"
#include "src/common/include/types.h"
#include "src/storage/include/buffer_manager.h"
#include "src/storage/include/file_handle.h"

using namespace graphflow::common;
using namespace std;

namespace graphflow {
namespace storage {

class ColumnBase {};

template<typename T, typename U>
class Column : public ColumnBase {

public:
    Column(const string path, uint64_t numElements, BufferManager &bufferManager)
        : elementSize(sizeof(T) + sizeof(U)), numElementsPerPage(PAGE_SIZE / elementSize),
          fileHandle(FileHandle(path, 1 + (numElements / numElementsPerPage))),
          bufferManager(bufferManager) {}

    inline void getVal(gfNodeOffset_t nodeOffset, gfLabel_t &label, gfNodeOffset_t &offset) {
        auto pageIdx = getPageIdx(nodeOffset);
        auto frame = bufferManager.pin(fileHandle, pageIdx);
        auto pageOffsetInFrame = frame + getPageOffset(nodeOffset);
        memcpy(&label, (void *)pageOffsetInFrame, sizeof(T));
        memcpy(&offset, (void *)(pageOffsetInFrame + sizeof(T)), sizeof(U));
        bufferManager.unpin(fileHandle, pageIdx);
    }

    inline uint64_t getPageIdx(gfNodeOffset_t nodeOffset) {
        return nodeOffset / numElementsPerPage;
    }

    inline uint32_t getPageOffset(gfNodeOffset_t nodeOffset) {
        return (nodeOffset % numElementsPerPage) * elementSize;
    }

private:
    size_t elementSize;
    uint32_t numElementsPerPage;
    FileHandle fileHandle;
    BufferManager &bufferManager;
};

template<typename T>
class Column<T, void> : public ColumnBase {

public:
    Column(const string path, uint64_t numElements, BufferManager &bufferManager)
        : elementSize(sizeof(T)), numElementsPerPage(PAGE_SIZE / elementSize),
          fileHandle(FileHandle(path, 1 + (numElements / numElementsPerPage))),
          bufferManager(bufferManager) {}

    inline void getVal(gfNodeOffset_t nodeOffset, T &t) {
        auto pageIdx = getPageIdx(nodeOffset);
        auto frame = bufferManager.pin(fileHandle, pageIdx);
        memcpy(&t, (void *)(frame + getPageOffset(nodeOffset)), elementSize);
        bufferManager.unpin(fileHandle, pageIdx);
    }

    inline uint64_t getPageIdx(gfNodeOffset_t nodeOffset) {
        return nodeOffset / numElementsPerPage;
    }

    inline uint32_t getPageOffset(gfNodeOffset_t nodeOffset) {
        return (nodeOffset % numElementsPerPage) * elementSize;
    }

private:
    size_t elementSize;
    uint32_t numElementsPerPage;
    FileHandle fileHandle;
    BufferManager &bufferManager;
};

typedef Column<gfInt_t, void> ColumnInteger;
typedef Column<gfDouble_t, void> ColumnDouble;
typedef Column<gfBool_t, void> ColumnBoolean;
typedef Column<gfString_t, void> ColumnString;

typedef Column<uint8_t, void> Column1BOffset;
typedef Column<uint16_t, void> Column2BOffset;
typedef Column<uint32_t, void> Column4BOffset;
typedef Column<uint64_t, void> Column8BOffset;

typedef Column<uint8_t, uint8_t> Column1BLabel1BOffset;
typedef Column<uint8_t, uint16_t> Column1BLabel2BOffset;
typedef Column<uint8_t, uint32_t> Column1BLabel4BOffset;
typedef Column<uint8_t, uint64_t> Column1BLabel8BOffset;

typedef Column<uint16_t, uint8_t> Column2BLabel1BOffset;
typedef Column<uint16_t, uint16_t> Column2BLabel2BOffset;
typedef Column<uint16_t, uint32_t> Column2BLabel4BOffset;
typedef Column<uint16_t, uint64_t> Column2BLabel8BOffset;

typedef Column<uint32_t, uint8_t> Column4BLabel1BOffset;
typedef Column<uint32_t, uint16_t> Column4BLabel2BOffset;
typedef Column<uint32_t, uint32_t> Column4BLabel4BOffset;
typedef Column<uint32_t, uint64_t> Column4BLabel8BOffset;

} // namespace storage
} // namespace graphflow

#endif // GRAPHFLOW_STORAGE_COLUMN_H_

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
        : elementSize(elementSize), numElementsPerPage(PAGE_SIZE / elementSize),
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
    size_t elementSize;
    uint32_t numElementsPerPage;
    FileHandle fileHandle;
    BufferManager &bufferManager;
};

template<typename T, typename U>
class Column : public ColumnBase {

public:
    Column(const string path, uint64_t numElements, BufferManager &bufferManager)
        : ColumnBase(path, sizeof(T) + sizeof(U), numElements, bufferManager){};

    inline void getVal(gfNodeOffset_t nodeOffset, gfLabel_t &label, gfNodeOffset_t &offset) {
        auto pageIdx = getPageIdx(nodeOffset, numElementsPerPage);
        auto frame = bufferManager.pin(fileHandle, pageIdx);
        auto pageOffsetInFrame = frame + getPageOffset(nodeOffset, numElementsPerPage, elementSize);
        memcpy(&label, (void *)pageOffsetInFrame, sizeof(T));
        memcpy(&offset, (void *)(pageOffsetInFrame + sizeof(T)), sizeof(U));
        bufferManager.unpin(fileHandle, pageIdx);
    }
};

template<typename T>
class Column<T, void> : public ColumnBase {

public:
    Column(const string propertyName, const string path, uint64_t numElements,
        BufferManager &bufferManager)
        : ColumnBase(path, sizeof(T), numElements, bufferManager), propertyName(propertyName){};

    inline void getVal(gfNodeOffset_t nodeOffset, T &t) {
        auto pageIdx = getPageIdx(nodeOffset, numElementsPerPage);
        auto frame = bufferManager.pin(fileHandle, pageIdx);
        memcpy(&t, (void *)(frame + getPageOffset(nodeOffset, numElementsPerPage, elementSize)),
            elementSize);
        bufferManager.unpin(fileHandle, pageIdx);
    }

    inline const string &getPropertyName() { return propertyName; };

private:
    const string propertyName;
};

typedef Column<gfInt_t, void> ColumnInteger;
typedef Column<gfDouble_t, void> ColumnDouble;
typedef Column<gfBool_t, void> ColumnBoolean;
typedef Column<gfString_t, void> ColumnString;

typedef Column<uint16_t, void> Column2BOffset;
typedef Column<uint32_t, void> Column4BOffset;
typedef Column<uint64_t, void> Column8BOffset;

typedef Column<uint8_t, uint16_t> Column1BLabel2BOffset;
typedef Column<uint8_t, uint32_t> Column1BLabel4BOffset;
typedef Column<uint8_t, uint64_t> Column1BLabel8BOffset;

typedef Column<uint16_t, uint16_t> Column2BLabel2BOffset;
typedef Column<uint16_t, uint32_t> Column2BLabel4BOffset;
typedef Column<uint16_t, uint64_t> Column2BLabel8BOffset;

typedef Column<uint32_t, uint16_t> Column4BLabel2BOffset;
typedef Column<uint32_t, uint32_t> Column4BLabel4BOffset;
typedef Column<uint32_t, uint64_t> Column4BLabel8BOffset;

} // namespace storage
} // namespace graphflow

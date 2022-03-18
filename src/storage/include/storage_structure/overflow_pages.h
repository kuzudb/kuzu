#pragma once

#include "fcntl.h"

#include "src/common/include/vector/value_vector.h"
#include "src/storage/include/buffer_manager.h"
#include "src/storage/include/file_handle.h"
#include "src/storage/include/storage_structure/lists/utils.h"
#include "src/storage/include/storage_structure/storage_structure_utils.h"

using namespace graphflow::common;

namespace graphflow {
namespace storage {

class OverflowPages {

public:
    explicit OverflowPages(const string& fName, BufferManager& bufferManager, bool isInMemory)
        : fileHandle{getOverflowPagesFName(fName), FileHandle::O_DefaultPagedExistingDBFile},
          bufferManager{bufferManager}, isInMemory{isInMemory} {
        if (isInMemory) {
            StorageStructureUtils::pinEachPageOfFile(fileHandle, bufferManager);
        }
    };

    ~OverflowPages() {
        if (isInMemory) {
            StorageStructureUtils::unpinEachPageOfFile(fileHandle, bufferManager);
        }
    }

    static inline string getOverflowPagesFName(const string& fName) {
        return fName + StorageConfig::OVERFLOW_FILE_SUFFIX;
    }

    void readStringsToVector(ValueVector& valueVector);
    void readStringToVector(gf_string_t& gfStr, OverflowBuffer& overflowBuffer);
    void readListsToVector(ValueVector& valueVector);
    string readString(const gf_string_t& str);
    vector<Literal> readList(const gf_list_t& listVal, DataType childDataType);

private:
    void readListToVector(gf_list_t& gfList, OverflowBuffer& overflowBuffer);

    FileHandle fileHandle;
    BufferManager& bufferManager;
    bool isInMemory;
};

} // namespace storage
} // namespace graphflow
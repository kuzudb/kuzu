#pragma once

#include "fcntl.h"

#include "src/common/include/vector/value_vector.h"
#include "src/storage/include/buffer_manager.h"
#include "src/storage/include/data_structure/lists/utils.h"
#include "src/storage/include/file_handle.h"

using namespace graphflow::common;

namespace graphflow {
namespace storage {

class StringOverflowPages {

public:
    explicit StringOverflowPages(const string& fName, BufferManager& bufferManager, bool isInMemory)
        : fileHandle{getStringOverflowPagesFName(fName),
              isInMemory ? FileHandle::O_InMemoryDefaultPaged : FileHandle::O_DiskBasedDefaultPage},
          bufferManager(bufferManager){};

    static string getStringOverflowPagesFName(const string& fName) {
        return fName + OVERFLOW_FILE_SUFFIX;
    }

    void readStringsToVector(ValueVector& valueVector);

    void readStringToVector(gf_string_t& gfStr, StringBuffer& stringBuffer);

    string readString(const gf_string_t& str);

private:
    static constexpr char OVERFLOW_FILE_SUFFIX[] = ".ovf";

    FileHandle fileHandle;
    BufferManager& bufferManager;
};

} // namespace storage
} // namespace graphflow
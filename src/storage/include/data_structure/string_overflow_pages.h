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
        : fileHandle{getStringOverflowPagesFName(fName), isInMemory},
          bufferManager(bufferManager){};

    static string getStringOverflowPagesFName(const string& fName) {
        return fName + OVERFLOW_FILE_SUFFIX;
    }

    void readStringsToVector(ValueVector& valueVector, BufferManagerMetrics& metrics);

    void readStringToVector(
        gf_string_t& gfStr, StringBuffer& stringBuffer, BufferManagerMetrics& metrics);

    string readString(const gf_string_t& str, BufferManagerMetrics& metrics);

private:
    static constexpr char OVERFLOW_FILE_SUFFIX[] = ".ovf";

    FileHandle fileHandle;
    BufferManager& bufferManager;
};

} // namespace storage
} // namespace graphflow
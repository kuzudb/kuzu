#pragma once

#include "fcntl.h"

#include "src/common/include/vector/value_vector.h"
#include "src/storage/include/buffer_manager.h"
#include "src/storage/include/file_handle.h"

using namespace graphflow::common;

namespace graphflow {
namespace storage {

class StringOverflowPages {

public:
    explicit StringOverflowPages(const string& fname, BufferManager& bufferManager)
        : fileHandle{getStringOverflowPagesFName(fname), O_RDWR}, bufferManager{bufferManager} {}

    static string getStringOverflowPagesFName(const string& fName) {
        return fName + OVERFLOW_FILE_SUFFIX;
    }

    void readStringsFromOverflowPages(ValueVector& valueVector, BufferManagerMetrics& metrics);

    void readAStringFromOverflowPages(
        ValueVector& valueVector, uint32_t pos, BufferManagerMetrics& metrics);

private:
    static constexpr char OVERFLOW_FILE_SUFFIX[] = ".ovf";

    FileHandle fileHandle;
    BufferManager& bufferManager;
};

} // namespace storage
} // namespace graphflow
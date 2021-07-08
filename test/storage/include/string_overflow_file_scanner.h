#pragma once

#include <memory>

#include "test/storage/include/file_scanner.h"

#include "src/common/include/configs.h"
#include "src/common/include/gf_string.h"
#include "src/storage/include/data_structure/utils.h"

using namespace graphflow::common;

namespace graphflow {
namespace storage {

class StringOverflowFileFileScanner : FileScanner {

public:
    StringOverflowFileFileScanner(const string& overflowFName) : FileScanner(overflowFName) {}

    string readLargeString(gf_string_t& str) {
        PageCursor cursor;
        str.getOverflowPtrInfo(cursor.idx, cursor.offset);
        return std::string(buffer.get() + cursor.idx * PAGE_SIZE + cursor.offset, str.len);
    };
};

} // namespace storage
} // namespace graphflow

#pragma once

#include "test/storage/include/file_scanners/file_scanner.h"

#include "src/common/include/configs.h"
#include "src/common/include/gf_string.h"
#include "src/storage/include/data_structure/data_structure.h"

using namespace graphflow::common;

namespace graphflow {
namespace storage {

using namespace std;

class StringOverflowFileFileScanner : FileScanner {

public:
    StringOverflowFileFileScanner(const string& overflowFName) : FileScanner(overflowFName) {}

    string readLargeString(const gf_string_t& str) const;

    static string readString(
        gf_string_t& mainString, const StringOverflowFileFileScanner& stringOverflowFileScanner);
};

} // namespace storage
} // namespace graphflow

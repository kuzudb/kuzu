#pragma once

#include <memory>

#include "src/common/include/file_utils.h"

using namespace graphflow::common;

namespace graphflow {
namespace storage {

class FileScanner {

public:
    FileScanner(const string& fName);

protected:
    unique_ptr<uint8_t[]> buffer;
};

} // namespace storage
} // namespace graphflow

#pragma once

#include "exception.h"

namespace kuzu {
namespace common {

class StorageException : public Exception {
public:
    explicit StorageException(const std::string& msg) : Exception("Storage exception: " + msg){};
};

} // namespace common
} // namespace kuzu

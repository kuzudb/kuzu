#pragma once

#include "exception.h"

namespace kuzu {
namespace common {

class CopyException : public Exception {
public:
    explicit CopyException(const std::string& msg) : Exception("Copy exception: " + msg){};
};

} // namespace common
} // namespace kuzu

#pragma once

#include "exception.h"

namespace kuzu {
namespace common {

class OverflowException : public Exception {
public:
    explicit OverflowException(const std::string& msg) : Exception("Overflow exception: " + msg) {}
};

} // namespace common
} // namespace kuzu

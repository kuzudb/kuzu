#pragma once

#include "common/api.h"
#include "exception.h"

namespace kuzu {
namespace common {

class KUZU_API OverflowException : public Exception {
public:
    explicit OverflowException(const std::string& msg) : Exception("Overflow exception: " + msg) {}
};

} // namespace common
} // namespace kuzu

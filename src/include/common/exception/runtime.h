#pragma once

#include "exception.h"

namespace kuzu {
namespace common {

class RuntimeException : public Exception {
public:
    explicit RuntimeException(const std::string& msg) : Exception("Runtime exception: " + msg){};
};

} // namespace common
} // namespace kuzu

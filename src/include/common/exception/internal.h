#pragma once

#include "exception.h"

namespace kuzu {
namespace common {

class InternalException : public Exception {
public:
    explicit InternalException(const std::string& msg) : Exception(msg){};
};

} // namespace common
} // namespace kuzu

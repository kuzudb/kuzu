#pragma once

#include "exception.h"

namespace kuzu {
namespace common {

class ConnectionException : public Exception {
public:
    explicit ConnectionException(const std::string& msg) : Exception(msg){};
};

} // namespace common
} // namespace kuzu

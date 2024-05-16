#pragma once

#include "common/api.h"
#include "exception.h"

namespace kuzu {
namespace common {

class KUZU_API InternalException : public Exception {
public:
    explicit InternalException(const std::string& msg) : Exception(msg) {};
};

} // namespace common
} // namespace kuzu

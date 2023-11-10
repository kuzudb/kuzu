#pragma once

#include "common/api.h"
#include "exception.h"

namespace kuzu {
namespace common {

class KUZU_API RuntimeException : public Exception {
public:
    explicit RuntimeException(const std::string& msg) : Exception("Runtime exception: " + msg){};
};

} // namespace common
} // namespace kuzu

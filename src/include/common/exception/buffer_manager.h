#pragma once

#include "exception.h"

namespace kuzu {
namespace common {

class BufferManagerException : public Exception {
public:
    explicit BufferManagerException(const std::string& msg)
        : Exception("Buffer manager exception: " + msg){};
};

} // namespace common
} // namespace kuzu

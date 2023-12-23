#pragma once

#include "exception.h"

namespace kuzu {
namespace common {

class KUZU_API IOException : public Exception {
public:
    explicit IOException(const std::string& msg) : Exception("IO exception: " + msg) {}
};

} // namespace common
} // namespace kuzu

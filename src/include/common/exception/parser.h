#pragma once

#include "common/api.h"
#include "exception.h"

namespace kuzu {
namespace common {

class KUZU_API ParserException : public Exception {
public:
    explicit ParserException(const std::string& msg) : Exception("Parser exception: " + msg){};
};

} // namespace common
} // namespace kuzu

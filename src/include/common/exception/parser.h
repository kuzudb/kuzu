#pragma once

#include "exception.h"

namespace kuzu {
namespace common {

class ParserException : public Exception {
public:
    explicit ParserException(const std::string& msg) : Exception("Parser exception: " + msg){};
};

} // namespace common
} // namespace kuzu

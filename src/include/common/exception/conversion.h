#pragma once

#include "exception.h"

namespace kuzu {
namespace common {

class ConversionException : public Exception {
public:
    explicit ConversionException(const std::string& msg)
        : Exception("Conversion exception: " + msg) {}
};

} // namespace common
} // namespace kuzu

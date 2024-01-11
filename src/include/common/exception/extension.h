#pragma once

#include "exception.h"

namespace kuzu {
namespace common {

class KUZU_API ExtensionException : public Exception {
public:
    explicit ExtensionException(const std::string& msg)
        : Exception("Extension exception: " + msg) {}
};

} // namespace common
} // namespace kuzu

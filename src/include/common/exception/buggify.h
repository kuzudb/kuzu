#pragma once

#include "common/api.h"
#include "exception.h"

namespace kuzu {
namespace common {

class KUZU_API BuggifyException : public Exception {
public:
    explicit BuggifyException(const std::string& msg) : Exception("[BUGGIFY] " + msg) {};
};

} // namespace common
} // namespace kuzu

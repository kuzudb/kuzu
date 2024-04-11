#pragma once

#include "common/api.h"
#include "exception.h"

namespace kuzu {
namespace common {

class KUZU_API ConnectionException : public Exception {
public:
    explicit ConnectionException(const std::string& msg) : Exception(msg){};
};

} // namespace common
} // namespace kuzu

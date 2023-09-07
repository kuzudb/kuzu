#pragma once

#include "exception.h"

namespace kuzu {
namespace common {

class NotImplementedException : public Exception {
public:
    explicit NotImplementedException(const std::string& msg) : Exception(msg){};
};

} // namespace common
} // namespace kuzu

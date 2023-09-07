#pragma once

#include "exception.h"

namespace kuzu {
namespace common {

class BinderException : public Exception {
public:
    explicit BinderException(const std::string& msg) : Exception("Binder exception: " + msg){};
};

} // namespace common
} // namespace kuzu

#pragma once

#include "exception.h"

namespace kuzu {
namespace common {

class TransactionManagerException : public Exception {
public:
    explicit TransactionManagerException(const std::string& msg) : Exception(msg){};
};

} // namespace common
} // namespace kuzu

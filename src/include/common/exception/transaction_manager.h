#pragma once

#include "common/api.h"
#include "exception.h"

namespace kuzu {
namespace common {

class KUZU_API TransactionManagerException : public Exception {
public:
    explicit TransactionManagerException(const std::string& msg) : Exception(msg){};
};

} // namespace common
} // namespace kuzu

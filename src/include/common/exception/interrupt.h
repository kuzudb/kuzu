#pragma once

#include "exception.h"

namespace kuzu {
namespace common {

class InterruptException : public Exception {
public:
    explicit InterruptException() : Exception("Interrupted."){};
};

} // namespace common
} // namespace kuzu

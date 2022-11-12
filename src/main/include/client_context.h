#pragma once

#include <cstdint>

namespace kuzu {
namespace main {

// Contain client side configuration.
// We make profiler associated per query, so profiler is not maintained in client context.
class ClientContext {
    friend class Connection;

public:
    explicit ClientContext() : numThreadsForExecution{1} {}

    ~ClientContext() = default;

private:
    uint64_t numThreadsForExecution;
};

} // namespace main
} // namespace kuzu

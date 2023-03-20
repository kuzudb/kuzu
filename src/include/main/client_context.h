#pragma once

#include <atomic>
#include <cstdint>

#include "common/api.h"

namespace kuzu {
namespace main {

/**
 * @brief Contain client side configuration. We make profiler associated per query, so profiler is
 * not maintained in client context.
 */
class ClientContext {
    friend class Connection;

public:
    /**
     * @brief Constructs the ClientContext object.
     */
    KUZU_API explicit ClientContext();
    /**
     * @brief Deconstructs the ClientContext object.
     */
    KUZU_API ~ClientContext() = default;

    /**
     * @brief Returns whether the current query is interrupted or not.
     */
    KUZU_API bool isInterrupted() const { return interrupted; }

private:
    uint64_t numThreadsForExecution;
    std::atomic<bool> interrupted;
};

} // namespace main
} // namespace kuzu

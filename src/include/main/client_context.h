#pragma once

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

private:
    uint64_t numThreadsForExecution;
};

} // namespace main
} // namespace kuzu

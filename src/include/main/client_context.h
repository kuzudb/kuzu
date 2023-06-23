#pragma once

#include <atomic>
#include <cstdint>
#include <memory>

#include "common/api.h"
#include "common/timer.h"
#include "main/kuzu_fwd.h"

namespace kuzu {
namespace main {

struct ActiveQuery {
    explicit ActiveQuery();
    std::atomic<bool> interrupted;
    common::Timer timer;
};

/**
 * @brief Contain client side configuration. We make profiler associated per query, so profiler is
 * not maintained in client context.
 */
class ClientContext {
    friend class Connection;
    friend class testing::TinySnbDDLTest;
    friend class testing::TinySnbCopyCSVTransactionTest;
    friend class ThreadsSetting;
    friend class TimeoutSetting;

public:
    explicit ClientContext();

    ~ClientContext() = default;

    inline void interrupt() { activeQuery->interrupted = true; }

    bool isInterrupted() const { return activeQuery->interrupted; }

    inline bool isTimeOut() {
        return isTimeOutEnabled() && activeQuery->timer.getElapsedTimeInMS() > timeoutInMS;
    }

    inline bool isTimeOutEnabled() const { return timeoutInMS != 0; }

    void startTimingIfEnabled();

private:
    uint64_t numThreadsForExecution;
    std::unique_ptr<ActiveQuery> activeQuery;
    uint64_t timeoutInMS;
};

} // namespace main
} // namespace kuzu

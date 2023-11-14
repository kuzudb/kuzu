#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>

#include "common/timer.h"
#include "main/kuzu_fwd.h"

namespace kuzu {

namespace binder {
class Binder;
}

namespace main {
class Database;

struct ActiveQuery {
    explicit ActiveQuery();
    std::atomic<bool> interrupted;
    common::Timer timer;

    void reset();
};

using replace_func_t = std::function<std::unique_ptr<common::Value>(common::Value*)>;

/**
 * @brief Contain client side configuration. We make profiler associated per query, so profiler is
 * not maintained in client context.
 */
class ClientContext {
    friend class Connection;
    friend class binder::Binder;
    friend class testing::TinySnbDDLTest;
    friend class testing::TinySnbCopyCSVTransactionTest;
    friend class ThreadsSetting;
    friend class TimeoutSetting;
    friend class VarLengthExtendMaxDepthSetting;
    friend class EnableSemiMaskSetting;

public:
    explicit ClientContext(Database* database);

    ~ClientContext();

    inline void interrupt() { activeQuery.interrupted = true; }

    bool isInterrupted() const { return activeQuery.interrupted; }

    inline bool isTimeOut() {
        return isTimeOutEnabled() && activeQuery.timer.getElapsedTimeInMS() > timeoutInMS;
    }

    inline bool isTimeOutEnabled() const { return timeoutInMS != 0; }

    inline bool isEnableSemiMask() const { return enableSemiMask; }

    void startTimingIfEnabled();

    std::string getCurrentSetting(std::string optionName);

    transaction::Transaction* getActiveTransaction() const;
    transaction::TransactionContext* getTransactionContext() const;

    inline void setReplaceFunc(replace_func_t replaceFunc) {
        this->replaceFunc = std::move(replaceFunc);
    }

private:
    inline void resetActiveQuery() { activeQuery.reset(); }

    uint64_t numThreadsForExecution;
    ActiveQuery activeQuery;
    uint64_t timeoutInMS;
    uint32_t varLengthExtendMaxDepth;
    std::unique_ptr<transaction::TransactionContext> transactionContext;
    bool enableSemiMask;
    replace_func_t replaceFunc;
};

} // namespace main
} // namespace kuzu

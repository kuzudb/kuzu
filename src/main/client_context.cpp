#include "main/client_context.h"

#include <thread>

#include "common/constants.h"

namespace kuzu {
namespace main {

ActiveQuery::ActiveQuery() : interrupted{false} {}

ClientContext::ClientContext()
    : numThreadsForExecution{std::thread::hardware_concurrency()},
      timeoutInMS{common::ClientContextConstants::TIMEOUT_IN_MS} {}

void ClientContext::startTimingIfEnabled() {
    if (isTimeOutEnabled()) {
        activeQuery->timer.start();
    }
}

} // namespace main
} // namespace kuzu

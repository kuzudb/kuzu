#include "main/client_context.h"

#include <thread>

#include "common/constants.h"
#include "main/db_config.h"

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

std::string ClientContext::getCurrentSetting(std::string optionName) {
    auto option = main::DBConfig::getOptionByName(optionName);
    if (option == nullptr) {
        throw common::RuntimeException{"Invalid option name: " + optionName + "."};
    }
    return option->getSetting(this);
}

} // namespace main
} // namespace kuzu

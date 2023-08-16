#include "main/client_context.h"

#include <thread>

#include "common/constants.h"
#include "main/db_config.h"

using namespace kuzu::common;

namespace kuzu {
namespace main {

ActiveQuery::ActiveQuery() : interrupted{false} {}

void ActiveQuery::reset() {
    interrupted = false;
    timer = Timer();
}

ClientContext::ClientContext()
    : numThreadsForExecution{std::thread::hardware_concurrency()},
      timeoutInMS{ClientContextConstants::TIMEOUT_IN_MS}, varLengthExtendMaxDepth{
                                                              VAR_LENGTH_EXTEND_MAX_DEPTH} {}

void ClientContext::startTimingIfEnabled() {
    if (isTimeOutEnabled()) {
        activeQuery.timer.start();
    }
}

std::string ClientContext::getCurrentSetting(std::string optionName) {
    auto option = main::DBConfig::getOptionByName(optionName);
    if (option == nullptr) {
        throw RuntimeException{"Invalid option name: " + optionName + "."};
    }
    return option->getSetting(this);
}

} // namespace main
} // namespace kuzu

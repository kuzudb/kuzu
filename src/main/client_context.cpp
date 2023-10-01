#include "main/client_context.h"

#include <thread>

#include "common/constants.h"
#include "common/exception/runtime.h"
#include "main/database.h"
#include "main/db_config.h"
#include "transaction/transaction_context.h"

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace main {

ActiveQuery::ActiveQuery() : interrupted{false} {}

void ActiveQuery::reset() {
    interrupted = false;
    timer = Timer();
}

ClientContext::ClientContext(Database* database)
    : numThreadsForExecution{std::thread::hardware_concurrency()},
      timeoutInMS{ClientContextConstants::TIMEOUT_IN_MS},
      varLengthExtendMaxDepth{DEFAULT_VAR_LENGTH_EXTEND_MAX_DEPTH}, enableSemiMask{
                                                                        DEFAULT_ENABLE_SEMI_MASK} {
    transactionContext = std::make_unique<TransactionContext>(database);
}

ClientContext::~ClientContext() {}

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

Transaction* ClientContext::getActiveTransaction() const {
    return transactionContext->getActiveTransaction();
}

TransactionContext* ClientContext::getTransactionContext() const {
    return transactionContext.get();
}

} // namespace main
} // namespace kuzu

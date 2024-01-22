#include "main/client_context.h"

#include "common/constants.h"
#include "common/exception/runtime.h"
#include "common/random_engine.h"
#include "common/string_utils.h"
#include "extension/extension.h"
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
    : numThreadsForExecution{database->systemConfig.maxNumThreads},
      timeoutInMS{ClientContextConstants::TIMEOUT_IN_MS},
      varLengthExtendMaxDepth{DEFAULT_VAR_LENGTH_EXTEND_MAX_DEPTH},
      enableSemiMask{DEFAULT_ENABLE_SEMI_MASK}, database{database} {
    transactionContext = std::make_unique<TransactionContext>(database);
    randomEngine = std::make_unique<common::RandomEngine>();
}

void ClientContext::startTimingIfEnabled() {
    if (isTimeOutEnabled()) {
        activeQuery.timer.start();
    }
}

Value ClientContext::getCurrentSetting(const std::string& optionName) {
    auto lowerCaseOptionName = optionName;
    StringUtils::toLower(lowerCaseOptionName);
    // Firstly, try to find in built-in options.
    auto option = main::DBConfig::getOptionByName(lowerCaseOptionName);
    if (option != nullptr) {
        return option->getSetting(this);
    }
    // Secondly, try to find in current client session.
    if (extensionOptionValues.contains(lowerCaseOptionName)) {
        return extensionOptionValues.at(lowerCaseOptionName);
    }
    // Lastly, find the default value in db config.
    auto defaultOption = database->extensionOptions->getExtensionOption(lowerCaseOptionName);
    if (defaultOption != nullptr) {
        return defaultOption->defaultValue;
    }
    throw RuntimeException{"Invalid option name: " + lowerCaseOptionName + "."};
}

transaction::Transaction* ClientContext::getTx() const {
    return transactionContext->getActiveTransaction();
}

TransactionContext* ClientContext::getTransactionContext() const {
    return transactionContext.get();
}

void ClientContext::setExtensionOption(std::string name, common::Value value) {
    StringUtils::toLower(name);
    extensionOptionValues.insert_or_assign(name, std::move(value));
}

VirtualFileSystem* ClientContext::getVFSUnsafe() const {
    return database->vfs.get();
}

std::string ClientContext::getExtensionDir() const {
    return common::stringFormat("{}/extension", database->databasePath);
}

storage::MemoryManager* ClientContext::getMemoryManager() {
    return database->memoryManager.get();
}

} // namespace main
} // namespace kuzu

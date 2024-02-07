#include "main/client_context.h"

#include "common/constants.h"
#include "common/exception/runtime.h"
#include "common/random_engine.h"
#include "common/string_utils.h"
#include "extension/extension.h"
#include "main/database.h"
#include "main/db_config.h"
#include "transaction/transaction_context.h"

#if defined(_WIN32)
#include "common/windows_utils.h"
#endif

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
    fileSearchPath = "";
#if defined(_WIN32)
    homeDirectory = getEnvVariable("USERPROFILE");
#else
    homeDirectory = getEnvVariable("HOME");
#endif
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
    return common::stringFormat("{}/.kuzu/extension", homeDirectory);
}

storage::MemoryManager* ClientContext::getMemoryManager() {
    return database->memoryManager.get();
}

catalog::Catalog* ClientContext::getCatalog() {
    return database->catalog.get();
}

std::string ClientContext::getEnvVariable(const std::string& name) {
#if defined(_WIN32)
    auto envValue = common::WindowsUtils::utf8ToUnicode(name.c_str());
    auto result = _wgetenv(envValue.c_str());
    if (!result) {
        return std::string();
    }
    return WindowsUtils::unicodeToUTF8(result);
#else
    const char* env = getenv(name.c_str()); // NOLINT(*-mt-unsafe)
    if (!env) {
        return std::string();
    }
    return env;
#endif
}

} // namespace main
} // namespace kuzu

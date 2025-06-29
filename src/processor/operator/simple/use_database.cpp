#include "processor/operator/simple/use_database.h"

#include "main/database_manager.h"
#include "processor/execution_context.h"

namespace kuzu {
namespace processor {

void UseDatabase::executeInternal(ExecutionContext* context) {
    auto clientContext = context->clientContext;
    auto dbManager = clientContext->getDatabaseManager();
    dbManager->setDefaultDatabase(dbName);
    appendMessage("Used database successfully.", clientContext->getMemoryManager());
}

std::string UseDatabasePrintInfo::toString() const {
    std::string result = "Database: ";
    result += dbName;
    return result;
}

} // namespace processor
} // namespace kuzu

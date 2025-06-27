#include "processor/operator/simple/detach_database.h"

#include "main/database.h"
#include "main/database_manager.h"
#include "processor/execution_context.h"

namespace kuzu {
namespace processor {

std::string DetatchDatabasePrintInfo::toString() const {
    return "Database: " + name;
}

void DetachDatabase::executeInternal(ExecutionContext* context) {
    auto clientContext = context->clientContext;
    auto dbManager = clientContext->getDatabaseManager();
    if (dbManager->hasAttachedDatabase(dbName) &&
        dbManager->getAttachedDatabase(dbName)->getDBType() == common::ATTACHED_KUZU_DB_TYPE) {
        clientContext->setDefaultDatabase(nullptr /* defaultDatabase */);
    }
    dbManager->detachDatabase(dbName);
    appendMessage("Detached database successfully.", clientContext->getMemoryManager());
}

} // namespace processor
} // namespace kuzu

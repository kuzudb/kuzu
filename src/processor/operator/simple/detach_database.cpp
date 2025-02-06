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
    auto dbManager = context->clientContext->getDatabaseManager();
    if (dbManager->hasAttachedDatabase(dbName) &&
        dbManager->getAttachedDatabase(dbName)->getDBType() == common::ATTACHED_KUZU_DB_TYPE) {
        context->clientContext->setDefaultDatabase(nullptr /* defaultDatabase */);
    }
    dbManager->detachDatabase(dbName);
}

std::string DetachDatabase::getOutputMsg() {
    return "Detached database successfully.";
}

} // namespace processor
} // namespace kuzu

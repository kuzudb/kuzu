#include "processor/operator/simple/detach_database.h"

#include "main/database.h"
#include "main/database_manager.h"

namespace kuzu {
namespace processor {

std::string DetatchDatabasePrintInfo::toString() const {
    return "Database: " + name;
}

void DetachDatabase::executeInternal(kuzu::processor::ExecutionContext* context) {
    auto dbManager = context->clientContext->getDatabaseManager();
    if (dbManager->getAttachedDatabase(dbName) != nullptr &&
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

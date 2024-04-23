#include "processor/operator/simple/use_database.h"

#include "main/database_manager.h"

namespace kuzu {
namespace processor {

void UseDatabase::executeInternal(kuzu::processor::ExecutionContext* context) {
    auto dbManager = context->clientContext->getDatabaseManager();
    dbManager->setDefaultDatabase(dbName);
}

std::string UseDatabase::getOutputMsg() {
    return "Used database successfully.";
}

} // namespace processor
} // namespace kuzu

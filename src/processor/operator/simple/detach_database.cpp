#include "processor/operator/simple/detach_database.h"

#include "main/database.h"
#include "main/database_manager.h"

namespace kuzu {
namespace processor {

void DetachDatabase::executeInternal(kuzu::processor::ExecutionContext* context) {
    auto dbManager = context->clientContext->getDatabaseManager();
    dbManager->detachDatabase(dbName);
}

std::string DetachDatabase::getOutputMsg() {
    return "Detached database successfully.";
}

} // namespace processor
} // namespace kuzu

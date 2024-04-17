#include "processor/operator/use_database.h"

#include "main/database_manager.h"

namespace kuzu {
namespace processor {

bool UseDatabase::getNextTuplesInternal(kuzu::processor::ExecutionContext* context) {
    auto dbManager = context->clientContext->getDatabaseManager();
    dbManager->setDefaultDatabase(dbName);
    return false;
}

} // namespace processor
} // namespace kuzu

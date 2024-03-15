#include "processor/operator/detach_database.h"

#include "main/database.h"
#include "main/database_manager.h"

namespace kuzu {
namespace processor {

bool DetachDatabase::getNextTuplesInternal(kuzu::processor::ExecutionContext* context) {
    auto dbManager = context->clientContext->getDatabase()->getDatabaseManagerUnsafe();
    dbManager->detachDatabase(dbName);
    return false;
}

} // namespace processor
} // namespace kuzu

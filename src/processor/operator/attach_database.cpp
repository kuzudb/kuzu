#include "processor/operator/attach_database.h"

#include "main/database.h"
#include "main/database_manager.h"
#include "storage/storage_extension.h"

namespace kuzu {
namespace processor {

bool AttachDatabase::getNextTuplesInternal(kuzu::processor::ExecutionContext* context) {
    for (auto& [name, storageExtension] :
        context->clientContext->getDatabase()->getStorageExtensions()) {
        if (storageExtension->canHandleDB(attachInfo.dbType)) {
            auto db = storageExtension->attach(attachInfo.dbAlias, attachInfo.dbPath,
                context->clientContext);
            context->clientContext->getDatabase()
                ->getDatabaseManagerUnsafe()
                ->registerAttachedDatabase(std::move(db));
        }
    }
    return false;
}

} // namespace processor
} // namespace kuzu

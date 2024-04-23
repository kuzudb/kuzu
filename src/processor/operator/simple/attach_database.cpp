#include "processor/operator/simple/attach_database.h"

#include "main/database.h"
#include "main/database_manager.h"
#include "storage/storage_extension.h"

namespace kuzu {
namespace processor {

void AttachDatabase::executeInternal(ExecutionContext* context) {
    auto client = context->clientContext;
    for (auto& [name, storageExtension] : client->getDatabase()->getStorageExtensions()) {
        if (storageExtension->canHandleDB(attachInfo.dbType)) {
            auto db = storageExtension->attach(attachInfo.dbAlias, attachInfo.dbPath, client);
            client->getDatabaseManager()->registerAttachedDatabase(std::move(db));
        }
    }
}

std::string AttachDatabase::getOutputMsg() {
    return "Attach database successfully.";
}

} // namespace processor
} // namespace kuzu

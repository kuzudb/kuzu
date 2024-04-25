#include "processor/operator/simple/attach_database.h"

#include "common/exception/runtime.h"
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
            return;
        }
    }
    auto errMsg = common::stringFormat("No loaded extension can handle database type: {}.",
        attachInfo.dbType);
    if (attachInfo.dbType == "duckdb") {
        errMsg += "\nDid you forget to load duckdb extension?\nYou can load it by: load "
                  "extension duckdb;";
    } else if (attachInfo.dbType == "postgres") {
        errMsg += "\nDid you forget to load postgres extension?\nYou can load it by: load "
                  "extension postgres;";
    }
    throw common::RuntimeException{errMsg};
}

std::string AttachDatabase::getOutputMsg() {
    return "Attached database successfully.";
}

} // namespace processor
} // namespace kuzu

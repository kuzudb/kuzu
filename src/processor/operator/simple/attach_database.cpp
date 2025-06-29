#include "processor/operator/simple/attach_database.h"

#include "common/exception/runtime.h"
#include "common/string_utils.h"
#include "main/attached_database.h"
#include "main/database.h"
#include "main/database_manager.h"
#include "processor/execution_context.h"
#include "storage/storage_extension.h"
#include "storage/storage_manager.h"

namespace kuzu {
namespace processor {

std::string AttachDatabasePrintInfo::toString() const {
    std::string result = "Database: ";
    if (!dbName.empty()) {
        result += dbName;
    } else {
        result += dbPath;
    }
    return result;
}

static std::string attachMessage() {
    return "Attached database successfully.";
}

void AttachDatabase::executeInternal(ExecutionContext* context) {
    auto client = context->clientContext;
    auto databaseManager = client->getDatabaseManager();
    auto memoryManager = client->getMemoryManager();
    if (common::StringUtils::getUpper(attachInfo.dbType) == common::ATTACHED_KUZU_DB_TYPE) {
        auto db = std::make_unique<main::AttachedKuzuDatabase>(attachInfo.dbPath,
            attachInfo.dbAlias, common::ATTACHED_KUZU_DB_TYPE, client);
        client->setDefaultDatabase(db.get());
        databaseManager->registerAttachedDatabase(std::move(db));
        appendMessage(attachMessage(), memoryManager);
        return;
    }
    for (auto& storageExtension : client->getDatabase()->getStorageExtensions()) {
        if (storageExtension->canHandleDB(attachInfo.dbType)) {
            auto db = storageExtension->attach(attachInfo.dbAlias, attachInfo.dbPath, client,
                attachInfo.options);
            databaseManager->registerAttachedDatabase(std::move(db));
            appendMessage(attachMessage(), memoryManager);
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

} // namespace processor
} // namespace kuzu

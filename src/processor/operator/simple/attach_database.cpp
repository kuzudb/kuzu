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

void AttachDatabase::executeInternal(ExecutionContext* context) {
    auto client = context->clientContext;
    if (common::StringUtils::getUpper(attachInfo.dbType) == common::ATTACHED_KUZU_DB_TYPE) {
        auto attachedKuzuDB = std::make_unique<main::AttachedKuzuDatabase>(attachInfo.dbPath,
            attachInfo.dbAlias, common::ATTACHED_KUZU_DB_TYPE, context->clientContext);
        client->setDefaultDatabase(attachedKuzuDB.get());
        client->getDatabaseManager()->registerAttachedDatabase(std::move(attachedKuzuDB));
        return;
    }
    for (auto& storageExtension : client->getDatabase()->getStorageExtensions()) {
        if (storageExtension->canHandleDB(attachInfo.dbType)) {
            auto db = storageExtension->attach(attachInfo.dbAlias, attachInfo.dbPath, client,
                attachInfo.options);
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

#include "processor/operator/ddl/create_table.h"

#include "common/string_format.h"
#include "storage/storage_manager.h"

using namespace kuzu::catalog;
using namespace kuzu::common;

namespace kuzu {
namespace processor {

void CreateTable::executeDDLInternal(ExecutionContext* context) {
    auto catalog = context->clientContext->getCatalog();
    switch (info.onConflict) {
    case common::ConflictAction::ON_CONFLICT_DO_NOTHING: {
        if (catalog->containsTable(context->clientContext->getTx(), info.tableName)) {
            return;
        }
    }
    default:
        break;
    }
    auto newTableID = catalog->createTableSchema(context->clientContext->getTx(), info);
    tableCreated = true;
    auto storageManager = context->clientContext->getStorageManager();
    storageManager->createTable(newTableID, catalog, context->clientContext);
}

std::string CreateTable::getOutputMsg() {
    if (tableCreated) {
        return stringFormat("Table {} has been created.", info.tableName);
    }
    return stringFormat("Table {} already exists.", info.tableName);
}

} // namespace processor
} // namespace kuzu

#include "processor/operator/ddl/create_table.h"

#include "common/string_format.h"
#include "storage/storage_manager.h"

using namespace kuzu::catalog;
using namespace kuzu::common;

namespace kuzu {
namespace processor {

void CreateTable::executeDDLInternal(ExecutionContext* context) {
    auto catalog = context->clientContext->getCatalog();
    auto newTableID = catalog->createTableSchema(context->clientContext->getTx(), info);
    if (newTableID == INVALID_TABLE_ID) {
        return;
    }
    auto storageManager = context->clientContext->getStorageManager();
    storageManager->createTable(newTableID, catalog, context->clientContext);
}

std::string CreateTable::getOutputMsg() {
    switch (info.onConflict) {
    case parser::OnConflictOperation::ERROR:
        return stringFormat("Table {} has been created.", info.tableName);
    case parser::OnConflictOperation::IGNORE:
        return stringFormat("Table {} already exists.", info.tableName);
    default:
        KU_UNREACHABLE;
    }
}

} // namespace processor
} // namespace kuzu

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
    auto storageManager = context->clientContext->getStorageManager();
    storageManager->createTable(newTableID, catalog, context->clientContext->getTx());
}

std::string CreateTable::getOutputMsg() {
    return stringFormat("Table {} has been created.", info.tableName);
}

} // namespace processor
} // namespace kuzu

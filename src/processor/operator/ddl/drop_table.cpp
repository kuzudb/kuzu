#include "processor/operator/ddl/drop_table.h"

#include "catalog/catalog.h"
#include "common/string_format.h"
#include "storage/storage_manager.h"
#include "storage/wal_replayer_utils.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

void DropTable::executeDDLInternal(ExecutionContext* context) {
    context->clientContext->getCatalog()->dropTableSchema(tableID);
}

std::string DropTable::getOutputMsg() {
    return stringFormat("Table: {} has been dropped.", tableName);
}

} // namespace processor
} // namespace kuzu

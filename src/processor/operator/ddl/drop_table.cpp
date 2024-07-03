#include "processor/operator/ddl/drop_table.h"

#include "catalog/catalog.h"
#include "common/string_format.h"

using namespace kuzu::catalog;
using namespace kuzu::common;

namespace kuzu {
namespace processor {

std::string DropTablePrintInfo::toString() const {
    return tableName;
}

void DropTable::executeDDLInternal(ExecutionContext* context) {
    context->clientContext->getCatalog()->dropTableSchema(context->clientContext->getTx(), tableID);
}

std::string DropTable::getOutputMsg() {
    return stringFormat("Table: {} has been dropped.", tableName);
}

} // namespace processor
} // namespace kuzu

#include "processor/operator/ddl/drop_table.h"

#include "common/string_format.h"

using namespace kuzu::catalog;
using namespace kuzu::common;

namespace kuzu {
namespace processor {

void DropTable::executeDDLInternal(ExecutionContext* /*context*/) {
    catalog->dropTableSchema(tableID);
}

std::string DropTable::getOutputMsg() {
    return stringFormat("Table: {} has been dropped.", tableName);
}

} // namespace processor
} // namespace kuzu

#include "processor/operator/ddl/drop_table.h"

#include "common/string_utils.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

void DropTable::executeDDLInternal(ExecutionContext* context) {
    catalog->dropTableSchema(tableID);
}

std::string DropTable::getOutputMsg() {
    auto tableSchema = catalog->getReadOnlyVersion()->getTableSchema(tableID);
    return StringUtils::string_format("{}Table: {} has been dropped.",
        tableSchema->isNodeTable ? "Node" : "Rel", tableSchema->tableName);
}

} // namespace processor
} // namespace kuzu

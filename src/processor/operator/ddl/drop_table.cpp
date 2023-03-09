#include "processor/operator/ddl/drop_table.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

void DropTable::executeDDLInternal() {
    catalog->dropTableSchema(tableID);
}

std::string DropTable::getOutputMsg() {
    auto tableSchema = catalog->getReadOnlyVersion()->getTableSchema(tableID);
    return StringUtils::string_format("{}Table: {} has been dropped.",
        tableSchema->isNodeTable ? "Node" : "Rel", tableSchema->tableName);
}

} // namespace processor
} // namespace kuzu

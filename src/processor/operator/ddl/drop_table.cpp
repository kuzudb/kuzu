#include "processor/operator/ddl/drop_table.h"

namespace kuzu {
namespace processor {

void DropTable::executeDDLInternal() {
    auto tableSchema = catalog->getReadOnlyVersion()->getTableSchema(tableID);
    catalog->removeTableSchema(tableSchema);
}

std::string DropTable::getOutputMsg() {
    auto tableSchema = catalog->getReadOnlyVersion()->getTableSchema(tableID);
    return StringUtils::string_format("%sTable: %s has been dropped.",
        tableSchema->isNodeTable ? "Node" : "Rel", tableSchema->tableName.c_str());
}

} // namespace processor
} // namespace kuzu

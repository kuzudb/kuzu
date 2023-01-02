#include "processor/operator/ddl/create_node_table.h"

namespace kuzu {
namespace processor {

string CreateNodeTable::execute() {
    auto newTableID = catalog->addNodeTableSchema(tableName, primaryKeyIdx, propertyNameDataTypes);
    nodesStatisticsAndDeletedIDs->addNodeStatisticsAndDeletedIDs(
        catalog->getWriteVersion()->getNodeTableSchema(newTableID));
    return StringUtils::string_format("NodeTable: %s has been created.", tableName.c_str());
}

} // namespace processor
} // namespace kuzu

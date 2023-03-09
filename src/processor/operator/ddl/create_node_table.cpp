#include "processor/operator/ddl/create_node_table.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

void CreateNodeTable::executeDDLInternal() {
    auto newTableID = catalog->addNodeTableSchema(tableName, primaryKeyIdx, propertyNameDataTypes);
    nodesStatistics->addNodeStatisticsAndDeletedIDs(
        catalog->getWriteVersion()->getNodeTableSchema(newTableID));
}

std::string CreateNodeTable::getOutputMsg() {
    return StringUtils::string_format("NodeTable: {} has been created.", tableName);
}

} // namespace processor
} // namespace kuzu

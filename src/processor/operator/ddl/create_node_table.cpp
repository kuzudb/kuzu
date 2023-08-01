#include "processor/operator/ddl/create_node_table.h"

#include "common/string_utils.h"
#include "storage/storage_manager.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

void CreateNodeTable::executeDDLInternal(ExecutionContext* context) {
    auto newTableID = catalog->addNodeTableSchema(tableName, primaryKeyIdx, properties);
    nodesStatistics->addNodeStatisticsAndDeletedIDs(
        catalog->getWriteVersion()->getNodeTableSchema(newTableID));
    auto tableSchema = catalog->getWriteVersion()->getNodeTableSchema(newTableID);
    for (auto& property : tableSchema->properties) {
        storageManager.initMetadataDAHInfo(property.dataType, property.metadataDAHInfo);
    }
}

std::string CreateNodeTable::getOutputMsg() {
    return StringUtils::string_format("NodeTable: {} has been created.", tableName);
}

} // namespace processor
} // namespace kuzu

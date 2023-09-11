#include "processor/operator/ddl/create_node_table.h"

#include "catalog/node_table_schema.h"
#include "common/string_utils.h"
#include "storage/storage_manager.h"

using namespace kuzu::catalog;
using namespace kuzu::common;

namespace kuzu {
namespace processor {

void CreateNodeTable::executeDDLInternal() {
    auto extraInfo = (binder::BoundExtraCreateNodeTableInfo*)info->extraInfo.get();
    for (auto& property : extraInfo->properties) {
        property->setMetadataDAHInfo(
            storageManager->createMetadataDAHInfo(*property->getDataType()));
    }
    auto newTableID = catalog->addNodeTableSchema(*info);
    auto newNodeTableSchema =
        reinterpret_cast<NodeTableSchema*>(catalog->getWriteVersion()->getTableSchema(newTableID));
    nodesStatistics->addNodeStatisticsAndDeletedIDs(newNodeTableSchema);
}

std::string CreateNodeTable::getOutputMsg() {
    return StringUtils::string_format("Node table: {} has been created.", info->tableName);
}

} // namespace processor
} // namespace kuzu

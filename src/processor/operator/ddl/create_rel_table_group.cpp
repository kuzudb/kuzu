#include "processor/operator/ddl/create_rel_table_group.h"

#include "catalog/rel_table_group_schema.h"
#include "catalog/rel_table_schema.h"
#include "common/string_format.h"
#include "storage/storage_manager.h"

using namespace kuzu::common;
using namespace kuzu::catalog;

namespace kuzu {
namespace processor {

void CreateRelTableGroup::executeDDLInternal() {
    auto newRelTableGroupID = catalog->addRelTableGroupSchema(*info);
    auto writeCatalog = catalog->getWriteVersion();
    auto newRelTableGroupSchema =
        (RelTableGroupSchema*)writeCatalog->getTableSchema(newRelTableGroupID);
    for (auto& relTableID : newRelTableGroupSchema->getRelTableIDs()) {
        auto newRelTableSchema = writeCatalog->getTableSchema(relTableID);
        storageManager->getRelsStatistics()->addTableStatistic((RelTableSchema*)newRelTableSchema);
    }
    // TODO(Ziyi): remove this when we can log variable size record. See also wal_record.h
    for (auto relTableID : newRelTableGroupSchema->getRelTableIDs()) {
        storageManager->getWAL()->logCreateRelTableRecord(relTableID);
    }
}

std::string CreateRelTableGroup::getOutputMsg() {
    return stringFormat("Rel table group: {} has been created.", info->tableName);
}

} // namespace processor
} // namespace kuzu

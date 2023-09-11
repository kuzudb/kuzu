#include "processor/operator/ddl/create_rel_table_group.h"

#include "catalog/rel_table_group_schema.h"
#include "catalog/rel_table_schema.h"
#include "common/string_utils.h"

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
        relsStatistics->addTableStatistic((RelTableSchema*)newRelTableSchema);
    }
}

std::string CreateRelTableGroup::getOutputMsg() {
    return StringUtils::string_format("Rel table group: {} has been created.", info->tableName);
}

} // namespace processor
} // namespace kuzu

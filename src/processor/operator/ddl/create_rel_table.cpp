#include "processor/operator/ddl/create_rel_table.h"

#include "catalog/rel_table_schema.h"
#include "common/string_utils.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::binder;

namespace kuzu {
namespace processor {

void CreateRelTable::executeDDLInternal() {
    auto newRelTableID = catalog->addRelTableSchema(*info);
    auto newRelTableSchema = reinterpret_cast<RelTableSchema*>(
        catalog->getWriteVersion()->getTableSchema(newRelTableID));
    relsStatistics->addTableStatistic(newRelTableSchema);
}

std::string CreateRelTable::getOutputMsg() {
    return StringUtils::string_format("Rel table: {} has been created.", info->tableName);
}

} // namespace processor
} // namespace kuzu

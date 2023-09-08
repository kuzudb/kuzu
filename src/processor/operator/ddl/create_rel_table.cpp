#include "processor/operator/ddl/create_rel_table.h"

#include "common/string_utils.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::binder;

namespace kuzu {
namespace processor {

void CreateRelTable::executeDDLInternal() {
    auto newRelTableID = catalog->addRelTableSchema(*info);
    auto newRelTableSchema =
        (RelTableSchema*)catalog->getWriteVersion()->getTableSchema(newRelTableID);
    relsStatistics->addTableStatistic(newRelTableSchema);
}

std::string CreateRelTable::getOutputMsg() {
    return StringUtils::string_format("Rel table: {} has been created.", info->tableName);
}

} // namespace processor
} // namespace kuzu

#include "processor/operator/ddl/create_rel_table.h"

#include "common/string_utils.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

void CreateRelTable::executeDDLInternal() {
    auto srcPKDataType =
        catalog->getReadOnlyVersion()->getNodeTableSchema(srcTableID)->getPrimaryKey().dataType;
    auto dstPKDataType =
        catalog->getReadOnlyVersion()->getNodeTableSchema(dstTableID)->getPrimaryKey().dataType;
    auto newRelTableID = catalog->addRelTableSchema(tableName, relMultiplicity, properties,
        srcTableID, dstTableID, srcPKDataType, dstPKDataType);
    relsStatistics->addTableStatistic(catalog->getWriteVersion()->getRelTableSchema(newRelTableID));
}

std::string CreateRelTable::getOutputMsg() {
    return StringUtils::string_format("RelTable: {} has been created.", tableName);
}

} // namespace processor
} // namespace kuzu

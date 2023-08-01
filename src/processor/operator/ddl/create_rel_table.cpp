#include "processor/operator/ddl/create_rel_table.h"

#include "common/string_utils.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

void CreateRelTable::executeDDLInternal() {
    auto srcPKDataType = catalog->getReadOnlyVersion()
                             ->getNodeTableSchema(srcTableID)
                             ->getPrimaryKey()
                             ->getDataType();
    auto dstPKDataType = catalog->getReadOnlyVersion()
                             ->getNodeTableSchema(dstTableID)
                             ->getPrimaryKey()
                             ->getDataType();
    auto newRelTableID = catalog->addRelTableSchema(tableName, relMultiplicity,
        catalog::Property::copyProperties(properties), srcTableID, dstTableID,
        srcPKDataType->copy(), dstPKDataType->copy());
    relsStatistics->addTableStatistic(catalog->getWriteVersion()->getRelTableSchema(newRelTableID));
}

std::string CreateRelTable::getOutputMsg() {
    return StringUtils::string_format("RelTable: {} has been created.", tableName);
}

} // namespace processor
} // namespace kuzu

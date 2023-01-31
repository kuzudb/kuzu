#include "processor/operator/ddl/create_rel_table.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

void CreateRelTable::executeDDLInternal() {
    auto newRelTableID = catalog->addRelTableSchema(
        tableName, relMultiplicity, propertyNameDataTypes, srcTableID, dstTableID);
    relsStatistics->addTableStatistic(catalog->getWriteVersion()->getRelTableSchema(newRelTableID));
}

std::string CreateRelTable::getOutputMsg() {
    return StringUtils::string_format("RelTable: %s has been created.", tableName.c_str());
}

} // namespace processor
} // namespace kuzu

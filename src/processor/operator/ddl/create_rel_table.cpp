#include "include/create_rel_table.h"

namespace graphflow {
namespace processor {

string CreateRelTable::execute() {
    auto newRelTableID = catalog->addRelTableSchema(
        tableName, relMultiplicity, propertyNameDataTypes, srcDstTableIDs);
    relsStatistics->addTableStatistic(catalog->getWriteVersion()->getRelTableSchema(newRelTableID));
    return StringUtils::string_format("RelTable: %s has been created.", tableName.c_str());
}

} // namespace processor
} // namespace graphflow

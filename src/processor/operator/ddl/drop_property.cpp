#include "processor/operator/ddl/drop_property.h"

namespace kuzu {
namespace processor {

string DropProperty::execute() {
    catalog->initCatalogContentForWriteTrxIfNecessary();
    auto tableSchema = catalog->getWriteVersion()->getTableSchema(tableID);
    auto tableName = tableSchema->tableName;
    auto propertyName = tableSchema->getPropertyName(propertyID);
    tableSchema->removeProperty(propertyID);
    return StringUtils::string_format(
        "%s column of table %s has been dropped.", propertyName.c_str(), tableName.c_str());
}

} // namespace processor
} // namespace kuzu

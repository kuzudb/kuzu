#include "processor/operator/ddl/drop_property.h"

namespace kuzu {
namespace processor {

void DropProperty::executeDDLInternal() {
    catalog->initCatalogContentForWriteTrxIfNecessary();
    auto tableSchema = catalog->getWriteVersion()->getTableSchema(tableID);
    tableSchema->removeProperty(propertyID);
}

std::string DropProperty::getOutputMsg() {
    return {"Drop Succeed."};
}

} // namespace processor
} // namespace kuzu

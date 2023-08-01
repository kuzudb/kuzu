#include "processor/operator/ddl/add_node_property.h"

namespace kuzu {
namespace processor {

void AddNodeProperty::executeDDLInternal() {
    AddProperty::executeDDLInternal();
    auto& property = catalog->getWriteVersion()->getNodeProperty(tableID, propertyName);
    storageManager.initMetadataDAHInfo(dataType, property.metadataDAHInfo);
}

} // namespace processor
} // namespace kuzu

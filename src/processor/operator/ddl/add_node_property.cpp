#include "processor/operator/ddl/add_node_property.h"

namespace kuzu {
namespace processor {

void AddNodeProperty::executeDDLInternal() {
    AddProperty::executeDDLInternal();
    auto property = catalog->getWriteVersion()->getNodeProperty(tableID, propertyName);
    property->setMetadataDAHInfo(storageManager.initMetadataDAHInfo(*dataType));
}

} // namespace processor
} // namespace kuzu

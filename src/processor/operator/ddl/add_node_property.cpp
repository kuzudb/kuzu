#include "processor/operator/ddl/add_node_property.h"

namespace kuzu {
namespace processor {

void AddNodeProperty::executeDDLInternal() {
    auto metadataDAHInfo = storageManager.createMetadataDAHInfo(*dataType);
    catalog->addNodeProperty(
        tableID, propertyName, std::move(dataType), std::move(metadataDAHInfo));
}

} // namespace processor
} // namespace kuzu

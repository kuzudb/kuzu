#include "catalog/catalog_entry/node_table_id_pair.h"

#include "common/serializer/deserializer.h"
#include "common/serializer/serializer.h"

using namespace kuzu::common;

namespace kuzu {
namespace catalog {

void NodeTableIDPair::serialize(Serializer& serializer) const {
    serializer.writeDebuggingInfo("srcTableID");
    serializer.serializeValue(srcTableID);
    serializer.writeDebuggingInfo("dstTableID");
    serializer.serializeValue(dstTableID);
}

NodeTableIDPair NodeTableIDPair::deserialize(Deserializer& deser) {
    std::string debuggingInfo;
    table_id_t srcTableID;
    table_id_t dstTableID;
    deser.validateDebuggingInfo(debuggingInfo, "srcTableID");
    deser.deserializeValue(srcTableID);
    deser.validateDebuggingInfo(debuggingInfo, "dstTableID");
    deser.deserializeValue(dstTableID);
    return NodeTableIDPair{srcTableID, dstTableID};
}

} // namespace catalog
} // namespace kuzu

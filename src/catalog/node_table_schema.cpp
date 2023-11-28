#include "catalog/node_table_schema.h"

#include <memory>
#include <unordered_set>

#include "common/serializer/deserializer.h"
#include "common/serializer/serializer.h"
#include "common/types/internal_id_t.h"
#include "common/types/types.h"

using namespace kuzu::common;

namespace kuzu {
namespace catalog {

void NodeTableSchema::serializeInternal(Serializer& serializer) {
    serializer.serializeValue(primaryKeyPropertyID);
    serializer.serializeUnorderedSet(fwdRelTableIDSet);
    serializer.serializeUnorderedSet(bwdRelTableIDSet);
}

std::unique_ptr<NodeTableSchema> NodeTableSchema::deserialize(Deserializer& deserializer) {
    property_id_t primaryKeyPropertyID;
    std::unordered_set<table_id_t> fwdRelTableIDSet;
    std::unordered_set<table_id_t> bwdRelTableIDSet;
    deserializer.deserializeValue(primaryKeyPropertyID);
    deserializer.deserializeUnorderedSet(fwdRelTableIDSet);
    deserializer.deserializeUnorderedSet(bwdRelTableIDSet);
    return std::make_unique<NodeTableSchema>(
        primaryKeyPropertyID, fwdRelTableIDSet, bwdRelTableIDSet);
}

} // namespace catalog
} // namespace kuzu

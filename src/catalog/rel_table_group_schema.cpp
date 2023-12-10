#include "catalog/rel_table_group_schema.h"

#include "common/serializer/deserializer.h"
#include "common/serializer/serializer.h"

using namespace kuzu::common;

namespace kuzu {
namespace catalog {

RelTableGroupSchema::RelTableGroupSchema(const RelTableGroupSchema& other) : TableSchema{other} {
    relTableIDs = other.relTableIDs;
}

void RelTableGroupSchema::serializeInternal(Serializer& serializer) {
    serializer.serializeVector(relTableIDs);
}

std::unique_ptr<RelTableGroupSchema> RelTableGroupSchema::deserialize(Deserializer& deserializer) {
    std::vector<table_id_t> relTableIDs;
    deserializer.deserializeVector(relTableIDs);
    auto schema = std::make_unique<RelTableGroupSchema>();
    schema->relTableIDs = std::move(relTableIDs);
    return schema;
}

} // namespace catalog
} // namespace kuzu

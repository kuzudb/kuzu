#include "catalog/rel_table_group_schema.h"

#include <memory>
#include <utility>
#include <vector>

#include "common/serializer/deserializer.h"
#include "common/serializer/serializer.h"
#include "common/types/internal_id_t.h"

using namespace kuzu::common;

namespace kuzu {
namespace catalog {

void RelTableGroupSchema::serializeInternal(Serializer& serializer) {
    serializer.serializeVector(relTableIDs);
}

std::unique_ptr<RelTableGroupSchema> RelTableGroupSchema::deserialize(Deserializer& deserializer) {
    std::vector<table_id_t> relTableIDs;
    deserializer.deserializeVector(relTableIDs);
    return std::make_unique<RelTableGroupSchema>(std::move(relTableIDs));
}

} // namespace catalog
} // namespace kuzu

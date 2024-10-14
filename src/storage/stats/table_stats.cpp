#include "storage/stats/table_stats.h"

#include "common/serializer/deserializer.h"
#include "common/serializer/serializer.h"

namespace kuzu {
namespace storage {

void TableStats::serialize(common::Serializer& serializer) const {
    serializer.writeDebuggingInfo("cardinality");
    serializer.write(cardinality);
}

TableStats TableStats::deserialize(common::Deserializer& deserializer) {
    std::string info;
    deserializer.validateDebuggingInfo(info, "cardinality");
    deserializer.deserializeValue(cardinality);
    return *this;
}

} // namespace storage
} // namespace kuzu

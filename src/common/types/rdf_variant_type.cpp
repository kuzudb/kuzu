#include "common/types/rdf_variant_type.h"

namespace kuzu {
namespace common {

std::unique_ptr<LogicalType> RdfVariantType::getType() {
    std::vector<StructField> fields;
    fields.emplace_back("_type", LogicalType::UINT8());
    fields.emplace_back("_value", LogicalType::BLOB());
    auto extraInfo = std::make_unique<StructTypeInfo>(std::move(fields));
    return LogicalType::RDF_VARIANT(std::move(extraInfo));
}

} // namespace common
} // namespace kuzu

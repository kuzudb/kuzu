#include "common/types/rdf_variant_type.h"

namespace kuzu {
namespace common {

std::unique_ptr<LogicalType> RdfVariantType::getType() {
    std::vector<std::unique_ptr<StructField>> fields;
    fields.push_back(std::make_unique<StructField>("_type", LogicalType::UINT8()));
    fields.push_back(std::make_unique<StructField>("_value", LogicalType::BLOB()));
    auto extraInfo = std::make_unique<StructTypeInfo>(std::move(fields));
    return std::make_unique<LogicalType>(LogicalTypeID::RDF_VARIANT, std::move(extraInfo));
}

} // namespace common
} // namespace kuzu

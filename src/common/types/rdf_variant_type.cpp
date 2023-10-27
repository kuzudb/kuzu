#include "common/types/rdf_variant_type.h"

namespace kuzu {
namespace common {

std::unique_ptr<LogicalType> RdfVariantType::getType() {
    std::vector<std::unique_ptr<StructField>> fields;
    fields.push_back(std::make_unique<StructField>(
        "_type", std::make_unique<LogicalType>(LogicalTypeID::UINT8)));
    fields.push_back(std::make_unique<StructField>(
        "_value", std::make_unique<LogicalType>(LogicalTypeID::BLOB)));
    auto extraInfo = std::make_unique<StructTypeInfo>(std::move(fields));
    return std::make_unique<LogicalType>(LogicalTypeID::RDF_VARIANT, std::move(extraInfo));
}

} // namespace common
} // namespace kuzu

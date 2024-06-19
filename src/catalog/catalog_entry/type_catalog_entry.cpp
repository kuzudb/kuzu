#include "catalog/catalog_entry/type_catalog_entry.h"

namespace kuzu {
namespace catalog {

void TypeCatalogEntry::serialize(common::Serializer& serializer) const {
    CatalogEntry::serialize(serializer);
    type.serialize(serializer);
}

std::unique_ptr<TypeCatalogEntry> TypeCatalogEntry::deserialize(
    common::Deserializer& deserializer) {
    auto typeCatalogEntry = std::make_unique<TypeCatalogEntry>();
    typeCatalogEntry->type = common::LogicalType::deserialize(deserializer);
    return typeCatalogEntry;
}

} // namespace catalog
} // namespace kuzu

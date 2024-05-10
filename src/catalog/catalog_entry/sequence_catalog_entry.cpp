#include "catalog/catalog_entry/sequence_catalog_entry.h"

#include <algorithm>

#include "binder/ddl/bound_create_sequence_info.h"

using namespace kuzu::binder;
using namespace kuzu::common;

namespace kuzu {
namespace catalog {

void SequenceCatalogEntry::serialize(common::Serializer& serializer) const {
    CatalogEntry::serialize(serializer);
    serializer.write(sequenceID);
}

std::unique_ptr<SequenceCatalogEntry> SequenceCatalogEntry::deserialize(
    common::Deserializer& deserializer) {
    common::table_id_t sequenceID;
    deserializer.deserializeValue(sequenceID);
    std::unique_ptr<SequenceCatalogEntry> result;
    result->sequenceID = sequenceID;
    return result;
}

void SequenceCatalogEntry::copyFrom(const CatalogEntry& other) {
    CatalogEntry::copyFrom(other);
    auto& otherSequence = ku_dynamic_cast<const CatalogEntry&, const SequenceCatalogEntry&>(other);
    sequenceID = otherSequence.sequenceID;
}

} // namespace catalog
} // namespace kuzu

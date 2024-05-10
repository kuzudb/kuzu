#pragma once

#include <vector>

#include "binder/ddl/bound_create_sequence_info.h"
#include "catalog/property.h"
#include "catalog_entry.h"

namespace kuzu {
namespace binder {
struct BoundExtraCreateCatalogEntryInfo;
struct BoundAlterInfo;
} // namespace binder

namespace catalog {

class KUZU_API SequenceCatalogEntry : public CatalogEntry {
public:
    //===--------------------------------------------------------------------===//
    // constructors
    //===--------------------------------------------------------------------===//
    SequenceCatalogEntry() = default;
    SequenceCatalogEntry(CatalogSet* set, std::string name, common::sequence_id_t sequenceID)
        : CatalogEntry{CatalogEntryType::SEQUENCE_ENTRY, std::move(name)}, set{set}, 
          sequenceID{sequenceID} {}

    //===--------------------------------------------------------------------===//
    // getter & setter
    //===--------------------------------------------------------------------===//
    common::sequence_id_t getSequenceID() const { return sequenceID; }

    //===--------------------------------------------------------------------===//
    // serialization & deserialization
    //===--------------------------------------------------------------------===//
    void serialize(common::Serializer& serializer) const override;
    static std::unique_ptr<SequenceCatalogEntry> deserialize(common::Deserializer& deserializer);
    // std::unique_ptr<SequenceCatalogEntry> copy() const;

protected:
    void copyFrom(const CatalogEntry& other) override;

protected:
    CatalogSet* set;
    common::sequence_id_t sequenceID;
};

} // namespace catalog
} // namespace kuzu

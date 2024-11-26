#pragma once

#include "catalog_entry.h"

namespace kuzu {
namespace catalog {

class KUZU_API IndexCatalogEntry : public CatalogEntry {
public:
    //===--------------------------------------------------------------------===//
    // constructors
    //===--------------------------------------------------------------------===//
    IndexCatalogEntry() = default;

    IndexCatalogEntry(common::table_id_t tableID, std::string indexName)
        : CatalogEntry{CatalogEntryType::INDEX_ENTRY,
              common::stringFormat("{}_{}", tableID, indexName)},
          tableID{tableID}, indexName{std::move(indexName)} {}

    common::table_id_t getTableID() const { return tableID; }

    //===--------------------------------------------------------------------===//
    // serialization & deserialization
    //===--------------------------------------------------------------------===//
    void serialize(common::Serializer& /*serializer*/) const override {}
    // TODO(Ziyi/Guodong) : If the database fails with loaded extensions, should we restart the db
    // and reload previously loaded extensions? Currently, we don't have the mechanism to reload
    // extensions during recovery, thus, we are not able to recover the indexes created by
    // extensions.
    static std::unique_ptr<IndexCatalogEntry> deserialize(common::Deserializer& /*deserializer*/) {
        KU_UNREACHABLE;
    }

    std::string toCypher(main::ClientContext* /*clientContext*/) const override { KU_UNREACHABLE; }
    virtual std::unique_ptr<IndexCatalogEntry> copy() const = 0;

    void copyFrom(const CatalogEntry& other) override {
        CatalogEntry::copyFrom(other);
        auto& otherTable = other.constCast<IndexCatalogEntry>();
        tableID = otherTable.tableID;
        indexName = otherTable.indexName;
    }

protected:
    common::table_id_t tableID = common::INVALID_TABLE_ID;
    std::string indexName;
};

} // namespace catalog
} // namespace kuzu

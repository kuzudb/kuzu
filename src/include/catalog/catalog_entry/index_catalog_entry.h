#pragma once

#include "catalog_entry.h"
#include "common/serializer/deserializer.h"

namespace kuzu {
namespace catalog {

class KUZU_API IndexCatalogEntry : public CatalogEntry {
public:
    //===--------------------------------------------------------------------===//
    // constructors
    //===--------------------------------------------------------------------===//
    IndexCatalogEntry() = default;

    IndexCatalogEntry(std::string type, common::table_id_t tableID, std::string indexName)
        : CatalogEntry{CatalogEntryType::INDEX_ENTRY,
              common::stringFormat("{}_{}", tableID, indexName)},
          type{std::move(type)}, tableID{tableID}, indexName{std::move(indexName)} {}

    std::string getIndexType() const { return type; }

    common::table_id_t getTableID() const { return tableID; }

    std::string getIndexName() const { return indexName; }

    uint8_t* getAuxBuffer() const { return auxBuffer.get(); }

    //===--------------------------------------------------------------------===//
    // serialization & deserialization
    //===--------------------------------------------------------------------===//
    void serialize(common::Serializer& serializer) const override {
        CatalogEntry::serialize(serializer);
        serializer.write(type);
        serializer.write(tableID);
        serializer.write(indexName);
        serializeAuxInfo(serializer);
    }
    static std::unique_ptr<IndexCatalogEntry> deserialize(common::Deserializer& deserializer) {
        std::string type;
        common::table_id_t tableID = common::INVALID_TABLE_ID;
        std::string indexName;
        deserializer.deserializeValue(type);
        deserializer.deserializeValue(tableID);
        deserializer.deserializeValue(indexName);
        auto indexEntry = std::make_unique<IndexCatalogEntry>(type, tableID, std::move(indexName));
        uint64_t auxBufferSize = 0;
        deserializer.deserializeValue(auxBufferSize);
        indexEntry->auxBuffer = std::make_unique<uint8_t[]>(auxBufferSize);
        deserializer.read(indexEntry->auxBuffer.get(), auxBufferSize);
        return indexEntry;
    }

    virtual void serializeAuxInfo(common::Serializer& /*serializer*/) const { KU_UNREACHABLE; }

    std::string toCypher(main::ClientContext* /*clientContext*/) const override { KU_UNREACHABLE; }
    virtual std::unique_ptr<IndexCatalogEntry> copy() const {
        return std::make_unique<IndexCatalogEntry>(type, tableID, indexName);
    }

    void copyFrom(const CatalogEntry& other) override {
        CatalogEntry::copyFrom(other);
        auto& otherTable = other.constCast<IndexCatalogEntry>();
        tableID = otherTable.tableID;
        indexName = otherTable.indexName;
    }

protected:
    std::string type;
    common::table_id_t tableID = common::INVALID_TABLE_ID;
    std::string indexName;
    std::unique_ptr<uint8_t[]> auxBuffer;
};

} // namespace catalog
} // namespace kuzu

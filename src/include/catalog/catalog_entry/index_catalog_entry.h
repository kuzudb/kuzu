#pragma once

#include "catalog_entry.h"
#include "common/serializer/deserializer.h"

namespace kuzu {
namespace catalog {

class KUZU_API IndexCatalogEntry : public CatalogEntry {
public:
    static std::string getInternalIndexName(common::table_id_t tableID, std::string indexName) {
        return common::stringFormat("{}_{}", tableID, std::move(indexName));
    }

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

    uint64_t getAuxBufferSize() const { return auxBufferSize; }

    //===--------------------------------------------------------------------===//
    // serialization & deserialization
    //===--------------------------------------------------------------------===//
    // When serializing index entries to disk, we first write the fields of the base class,
    // followed by the size (in bytes) of the auxiliary data and its content.
    void serialize(common::Serializer& serializer) const override;
    // During deserialization of index entries from disk, we first read the base class
    // (IndexCatalogEntry). The auxiliary data is stored in auxBuffer, with its size in
    // auxBufferSize. Once the extension is loaded, the corresponding indexes are reconstructed
    // using the auxBuffer.
    static std::unique_ptr<IndexCatalogEntry> deserialize(common::Deserializer& deserializer);

    virtual void serializeAuxInfo(common::Serializer& /*serializer*/) const { KU_UNREACHABLE; }

    std::string toCypher(main::ClientContext* /*clientContext*/) const override { KU_UNREACHABLE; }
    virtual std::unique_ptr<IndexCatalogEntry> copy() const {
        return std::make_unique<IndexCatalogEntry>(type, tableID, indexName);
    }

    void copyFrom(const CatalogEntry& other) override;

protected:
    std::string type;
    common::table_id_t tableID = common::INVALID_TABLE_ID;
    std::string indexName;
    std::unique_ptr<uint8_t[]> auxBuffer;
    uint64_t auxBufferSize = 0;
};

} // namespace catalog
} // namespace kuzu

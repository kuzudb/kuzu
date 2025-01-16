#pragma once

#include "catalog_entry.h"
#include "common/serializer/deserializer.h"

namespace kuzu::common {
struct BufferReader;
}
namespace kuzu::common {
class BufferedSerializer;
}
namespace kuzu {
namespace catalog {

class IndexCatalogEntry;
struct KUZU_API IndexAuxInfo {
    virtual ~IndexAuxInfo() = default;
    virtual std::shared_ptr<common::BufferedSerializer> serialize() const;

    virtual std::unique_ptr<IndexAuxInfo> copy() = 0;

    template<typename TARGET>
    TARGET& cast() {
        return dynamic_cast<TARGET&>(*this);
    }
    template<typename TARGET>
    const TARGET& cast() const {
        return dynamic_cast<const TARGET&>(*this);
    }

    virtual std::string toCypher(const IndexCatalogEntry& indexEntry,
        main::ClientContext* context) = 0;
};

class KUZU_API IndexCatalogEntry final : public CatalogEntry {
public:
    static std::string getInternalIndexName(common::table_id_t tableID, std::string indexName) {
        return common::stringFormat("{}_{}", tableID, std::move(indexName));
    }

    IndexCatalogEntry(std::string type, common::table_id_t tableID, std::string indexName,
        std::unique_ptr<IndexAuxInfo> auxInfo)
        : CatalogEntry{CatalogEntryType::INDEX_ENTRY,
              common::stringFormat("{}_{}", tableID, indexName)},
          type{std::move(type)}, tableID{tableID}, indexName{std::move(indexName)},
          auxInfo{std::move(auxInfo)} {}

    std::string getIndexType() const { return type; }

    common::table_id_t getTableID() const { return tableID; }

    std::string getIndexName() const { return indexName; }

    // When serializing index entries to disk, we first write the fields of the base class,
    // followed by the size (in bytes) of the auxiliary data and its content.
    void serialize(common::Serializer& serializer) const override;
    // During deserialization of index entries from disk, we first read the base class
    // (IndexCatalogEntry). The auxiliary data is stored in auxBuffer, with its size in
    // auxBufferSize. Once the extension is loaded, the corresponding indexes are reconstructed
    // using the auxBuffer.
    static std::unique_ptr<IndexCatalogEntry> deserialize(common::Deserializer& deserializer);

    std::string toCypher(main::ClientContext* context) const override {
        return auxInfo->toCypher(*this, context);
    }
    std::unique_ptr<IndexCatalogEntry> copy() const {
        return std::make_unique<IndexCatalogEntry>(type, tableID, indexName, auxInfo->copy());
    }

    void copyFrom(const CatalogEntry& other) override;

    std::unique_ptr<common::BufferReader> getAuxBufferReader() const;

    void setAuxInfo(std::unique_ptr<IndexAuxInfo> auxInfo_) { auxInfo = std::move(auxInfo_); }
    const IndexAuxInfo& getAuxInfo() const { return *auxInfo; }

protected:
    std::string type;
    common::table_id_t tableID = common::INVALID_TABLE_ID;
    std::string indexName;
    std::unique_ptr<uint8_t[]> auxBuffer;
    std::unique_ptr<IndexAuxInfo> auxInfo;
    uint64_t auxBufferSize = 0;
};

} // namespace catalog
} // namespace kuzu

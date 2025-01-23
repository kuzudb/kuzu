#include "catalog/catalog_entry/index_catalog_entry.h"

#include "catalog/catalog_entry/hnsw_index_catalog_entry.h"
#include "common/exception/runtime.h"
#include "common/serializer/buffered_serializer.h"

namespace kuzu {
namespace catalog {

std::shared_ptr<common::BufferedSerializer> IndexAuxInfo::serialize() const {
    return std::make_shared<common::BufferedSerializer>(0 /*maximumSize*/);
}

void IndexCatalogEntry::setAuxInfo(std::unique_ptr<IndexAuxInfo> auxInfo_) {
    auxInfo = std::move(auxInfo_);
    auxBuffer = nullptr;
    auxBufferSize = 0;
}

void IndexCatalogEntry::serialize(common::Serializer& serializer) const {
    CatalogEntry::serialize(serializer);
    serializer.write(type);
    serializer.write(tableID);
    serializer.write(indexName);
    serializer.serializeVector(propertyIDs);
    if (isLoaded()) {
        const auto bufferedWriter = auxInfo->serialize();
        serializer.write<uint64_t>(bufferedWriter->getSize());
        serializer.write(bufferedWriter->getData().data.get(), bufferedWriter->getSize());
    } else {
        serializer.write(auxBufferSize);
        serializer.write(auxBuffer.get(), auxBufferSize);
    }
}

std::unique_ptr<IndexCatalogEntry> IndexCatalogEntry::deserialize(
    common::Deserializer& deserializer) {
    std::string type;
    common::table_id_t tableID = common::INVALID_TABLE_ID;
    std::string indexName;
    std::vector<common::property_id_t> propertyIDs;
    deserializer.deserializeValue(type);
    deserializer.deserializeValue(tableID);
    deserializer.deserializeValue(indexName);
    deserializer.deserializeVector(propertyIDs);
    auto indexEntry = std::make_unique<IndexCatalogEntry>(type, tableID, std::move(indexName),
        std::move(propertyIDs), nullptr /* auxInfo */);
    uint64_t auxBufferSize = 0;
    deserializer.deserializeValue(auxBufferSize);
    indexEntry->auxBuffer = std::make_unique<uint8_t[]>(auxBufferSize);
    indexEntry->auxBufferSize = auxBufferSize;
    deserializer.read(indexEntry->auxBuffer.get(), auxBufferSize);
    if (type == HNSWIndexCatalogEntry::TYPE_NAME) {
        indexEntry->setAuxInfo(HNSWIndexAuxInfo::deserialize(indexEntry->getAuxBufferReader()));
    }
    return indexEntry;
}

void IndexCatalogEntry::copyFrom(const CatalogEntry& other) {
    CatalogEntry::copyFrom(other);
    auto& otherTable = other.constCast<IndexCatalogEntry>();
    tableID = otherTable.tableID;
    indexName = otherTable.indexName;
    if (auxInfo) {
        auxInfo = otherTable.auxInfo->copy();
    }
}
std::unique_ptr<common::BufferReader> IndexCatalogEntry::getAuxBufferReader() const {
    // LCOV_EXCL_START
    if (!auxBuffer) {
        throw common::RuntimeException(
            common::stringFormat("Auxiliary buffer for index \"%s\" is not set.", indexName));
    }
    // LCOV_EXCL_STOP
    return std::make_unique<common::BufferReader>(auxBuffer.get(), auxBufferSize);
}

} // namespace catalog
} // namespace kuzu

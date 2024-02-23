#include "storage/stats/rel_table_statistics.h"

#include "catalog/catalog_entry/rel_table_catalog_entry.h"
#include "common/serializer/deserializer.h"
#include "common/serializer/serializer.h"
#include "storage/stats/table_statistics_collection.h"
#include "storage/wal/wal.h"

using namespace kuzu::catalog;
using namespace kuzu::common;

namespace kuzu {
namespace storage {

RelTableStats::RelTableStats(BMFileHandle* metadataFH, const catalog::TableCatalogEntry& tableEntry,
    BufferManager* bufferManager, WAL* wal)
    : TableStatistics{tableEntry}, nextRelOffset{0} {
    const auto& relTableEntry =
        ku_dynamic_cast<const TableCatalogEntry&, const RelTableCatalogEntry&>(tableEntry);
    if (!relTableEntry.isSingleMultiplicity(RelDataDirection::FWD)) {
        fwdCSROffsetMetadataDAHInfo = TablesStatistics::createMetadataDAHInfo(
            LogicalType{LogicalTypeID::INT64}, *metadataFH, bufferManager, wal);
        fwdCSRLengthMetadataDAHInfo = TablesStatistics::createMetadataDAHInfo(
            LogicalType{LogicalTypeID::INT64}, *metadataFH, bufferManager, wal);
    }
    if (!relTableEntry.isSingleMultiplicity(RelDataDirection::BWD)) {
        bwdCSROffsetMetadataDAHInfo = TablesStatistics::createMetadataDAHInfo(
            LogicalType{LogicalTypeID::INT64}, *metadataFH, bufferManager, wal);
        bwdCSRLengthMetadataDAHInfo = TablesStatistics::createMetadataDAHInfo(
            LogicalType{LogicalTypeID::INT64}, *metadataFH, bufferManager, wal);
    }
    fwdAdjMetadataDAHInfo = TablesStatistics::createMetadataDAHInfo(
        LogicalType{LogicalTypeID::INTERNAL_ID}, *metadataFH, bufferManager, wal);
    bwdAdjMetadataDAHInfo = TablesStatistics::createMetadataDAHInfo(
        LogicalType{LogicalTypeID::INTERNAL_ID}, *metadataFH, bufferManager, wal);
    fwdPropertyMetadataDAHInfos.clear();
    bwdPropertyMetadataDAHInfos.clear();
    fwdPropertyMetadataDAHInfos.reserve(tableEntry.getNumProperties());
    bwdPropertyMetadataDAHInfos.reserve(tableEntry.getNumProperties());
    for (auto& property : tableEntry.getPropertiesRef()) {
        fwdPropertyMetadataDAHInfos.push_back(TablesStatistics::createMetadataDAHInfo(
            *property.getDataType(), *metadataFH, bufferManager, wal));
        bwdPropertyMetadataDAHInfos.push_back(TablesStatistics::createMetadataDAHInfo(
            *property.getDataType(), *metadataFH, bufferManager, wal));
    }
}

RelTableStats::RelTableStats(const RelTableStats& other) : TableStatistics{other} {
    nextRelOffset = other.nextRelOffset;
    if (other.fwdCSROffsetMetadataDAHInfo) {
        fwdCSROffsetMetadataDAHInfo = other.fwdCSROffsetMetadataDAHInfo->copy();
        fwdCSRLengthMetadataDAHInfo = other.fwdCSRLengthMetadataDAHInfo->copy();
    }
    if (other.bwdCSROffsetMetadataDAHInfo) {
        bwdCSROffsetMetadataDAHInfo = other.bwdCSROffsetMetadataDAHInfo->copy();
        bwdCSRLengthMetadataDAHInfo = other.bwdCSRLengthMetadataDAHInfo->copy();
    }
    fwdAdjMetadataDAHInfo = other.fwdAdjMetadataDAHInfo->copy();
    bwdAdjMetadataDAHInfo = other.bwdAdjMetadataDAHInfo->copy();
    fwdPropertyMetadataDAHInfos.clear();
    fwdPropertyMetadataDAHInfos.reserve(other.fwdPropertyMetadataDAHInfos.size());
    for (auto& metadataDAHInfo : other.fwdPropertyMetadataDAHInfos) {
        fwdPropertyMetadataDAHInfos.push_back(metadataDAHInfo->copy());
    }
    bwdPropertyMetadataDAHInfos.clear();
    bwdPropertyMetadataDAHInfos.reserve(other.bwdPropertyMetadataDAHInfos.size());
    for (auto& metadataDAHInfo : other.bwdPropertyMetadataDAHInfos) {
        bwdPropertyMetadataDAHInfos.push_back(metadataDAHInfo->copy());
    }
}

void RelTableStats::serializeInternal(Serializer& serializer) {
    serializer.serializeValue(nextRelOffset);
    serializer.serializeOptionalValue(fwdCSROffsetMetadataDAHInfo);
    serializer.serializeOptionalValue(bwdCSROffsetMetadataDAHInfo);
    serializer.serializeOptionalValue(fwdCSRLengthMetadataDAHInfo);
    serializer.serializeOptionalValue(bwdCSRLengthMetadataDAHInfo);
    fwdAdjMetadataDAHInfo->serialize(serializer);
    bwdAdjMetadataDAHInfo->serialize(serializer);
    serializer.serializeVectorOfPtrs(fwdPropertyMetadataDAHInfos);
    serializer.serializeVectorOfPtrs(bwdPropertyMetadataDAHInfos);
}

std::unique_ptr<RelTableStats> RelTableStats::deserialize(
    uint64_t numRels, table_id_t tableID, Deserializer& deserializer) {
    offset_t nextRelOffset;
    deserializer.deserializeValue(nextRelOffset);
    std::unique_ptr<MetadataDAHInfo> fwdCSROffsetMetadataDAHInfo, bwdCSROffsetMetadataDAHInfo,
        fwdCSRLengthMetadataDAHInfo, bwdCSRLengthMetadataDAHInfo;
    deserializer.deserializeOptionalValue(fwdCSROffsetMetadataDAHInfo);
    deserializer.deserializeOptionalValue(bwdCSROffsetMetadataDAHInfo);
    deserializer.deserializeOptionalValue(fwdCSRLengthMetadataDAHInfo);
    deserializer.deserializeOptionalValue(bwdCSRLengthMetadataDAHInfo);
    auto fwdNbrIDMetadataDAHInfo = MetadataDAHInfo::deserialize(deserializer);
    auto bwdNbrIDMetadataDAHInfo = MetadataDAHInfo::deserialize(deserializer);
    std::vector<std::unique_ptr<MetadataDAHInfo>> fwdPropertyMetadataDAHInfos;
    std::vector<std::unique_ptr<MetadataDAHInfo>> bwdPropertyMetadataDAHInfos;
    deserializer.deserializeVectorOfPtrs(fwdPropertyMetadataDAHInfos);
    deserializer.deserializeVectorOfPtrs(bwdPropertyMetadataDAHInfos);
    auto result = std::make_unique<RelTableStats>(numRels, tableID, nextRelOffset);
    result->fwdCSROffsetMetadataDAHInfo = std::move(fwdCSROffsetMetadataDAHInfo);
    result->bwdCSROffsetMetadataDAHInfo = std::move(bwdCSROffsetMetadataDAHInfo);
    result->fwdCSRLengthMetadataDAHInfo = std::move(fwdCSRLengthMetadataDAHInfo);
    result->bwdCSRLengthMetadataDAHInfo = std::move(bwdCSRLengthMetadataDAHInfo);
    result->fwdAdjMetadataDAHInfo = std::move(fwdNbrIDMetadataDAHInfo);
    result->bwdAdjMetadataDAHInfo = std::move(bwdNbrIDMetadataDAHInfo);
    result->fwdPropertyMetadataDAHInfos = std::move(fwdPropertyMetadataDAHInfos);
    result->bwdPropertyMetadataDAHInfos = std::move(bwdPropertyMetadataDAHInfos);
    return result;
}

} // namespace storage
} // namespace kuzu

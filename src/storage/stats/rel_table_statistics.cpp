#include "storage/stats/rel_table_statistics.h"

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
    fwdCSROffsetMetadataDAHInfo = TablesStatistics::createMetadataDAHInfo(
        LogicalType{LogicalTypeID::INT64}, *metadataFH, bufferManager, wal);
    fwdCSRLengthMetadataDAHInfo = TablesStatistics::createMetadataDAHInfo(
        LogicalType{LogicalTypeID::INT64}, *metadataFH, bufferManager, wal);
    bwdCSROffsetMetadataDAHInfo = TablesStatistics::createMetadataDAHInfo(
        LogicalType{LogicalTypeID::INT64}, *metadataFH, bufferManager, wal);
    bwdCSRLengthMetadataDAHInfo = TablesStatistics::createMetadataDAHInfo(
        LogicalType{LogicalTypeID::INT64}, *metadataFH, bufferManager, wal);
    KU_ASSERT(fwdMetadataDAHInfos.empty() && bwdMetadataDAHInfos.empty());
    fwdMetadataDAHInfos.reserve(tableEntry.getNumProperties() + 1);
    bwdMetadataDAHInfos.reserve(tableEntry.getNumProperties() + 1);
    fwdMetadataDAHInfos.push_back(TablesStatistics::createMetadataDAHInfo(
        LogicalType{LogicalTypeID::INTERNAL_ID}, *metadataFH, bufferManager, wal));
    bwdMetadataDAHInfos.push_back(TablesStatistics::createMetadataDAHInfo(
        LogicalType{LogicalTypeID::INTERNAL_ID}, *metadataFH, bufferManager, wal));
    for (auto& property : tableEntry.getPropertiesRef()) {
        fwdMetadataDAHInfos.push_back(TablesStatistics::createMetadataDAHInfo(
            *property.getDataType(), *metadataFH, bufferManager, wal));
        bwdMetadataDAHInfos.push_back(TablesStatistics::createMetadataDAHInfo(
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
    fwdMetadataDAHInfos.clear();
    fwdMetadataDAHInfos.reserve(other.fwdMetadataDAHInfos.size());
    for (auto& metadataDAHInfo : other.fwdMetadataDAHInfos) {
        fwdMetadataDAHInfos.push_back(metadataDAHInfo->copy());
    }
    bwdMetadataDAHInfos.clear();
    bwdMetadataDAHInfos.reserve(other.bwdMetadataDAHInfos.size());
    for (auto& metadataDAHInfo : other.bwdMetadataDAHInfos) {
        bwdMetadataDAHInfos.push_back(metadataDAHInfo->copy());
    }
}

void RelTableStats::serializeInternal(Serializer& serializer) {
    serializer.serializeValue(nextRelOffset);
    serializer.serializeOptionalValue(fwdCSROffsetMetadataDAHInfo);
    serializer.serializeOptionalValue(bwdCSROffsetMetadataDAHInfo);
    serializer.serializeOptionalValue(fwdCSRLengthMetadataDAHInfo);
    serializer.serializeOptionalValue(bwdCSRLengthMetadataDAHInfo);
    serializer.serializeVectorOfPtrs(fwdMetadataDAHInfos);
    serializer.serializeVectorOfPtrs(bwdMetadataDAHInfos);
}

std::unique_ptr<RelTableStats> RelTableStats::deserialize(uint64_t numRels, table_id_t tableID,
    Deserializer& deserializer) {
    offset_t nextRelOffset;
    deserializer.deserializeValue(nextRelOffset);
    std::unique_ptr<MetadataDAHInfo> fwdCSROffsetMetadataDAHInfo, bwdCSROffsetMetadataDAHInfo,
        fwdCSRLengthMetadataDAHInfo, bwdCSRLengthMetadataDAHInfo;
    deserializer.deserializeOptionalValue(fwdCSROffsetMetadataDAHInfo);
    deserializer.deserializeOptionalValue(bwdCSROffsetMetadataDAHInfo);
    deserializer.deserializeOptionalValue(fwdCSRLengthMetadataDAHInfo);
    deserializer.deserializeOptionalValue(bwdCSRLengthMetadataDAHInfo);
    std::vector<std::unique_ptr<MetadataDAHInfo>> fwdPropertyMetadataDAHInfos;
    std::vector<std::unique_ptr<MetadataDAHInfo>> bwdPropertyMetadataDAHInfos;
    deserializer.deserializeVectorOfPtrs(fwdPropertyMetadataDAHInfos);
    deserializer.deserializeVectorOfPtrs(bwdPropertyMetadataDAHInfos);
    auto result = std::make_unique<RelTableStats>(numRels, tableID, nextRelOffset);
    result->fwdCSROffsetMetadataDAHInfo = std::move(fwdCSROffsetMetadataDAHInfo);
    result->bwdCSROffsetMetadataDAHInfo = std::move(bwdCSROffsetMetadataDAHInfo);
    result->fwdCSRLengthMetadataDAHInfo = std::move(fwdCSRLengthMetadataDAHInfo);
    result->bwdCSRLengthMetadataDAHInfo = std::move(bwdCSRLengthMetadataDAHInfo);
    result->fwdMetadataDAHInfos = std::move(fwdPropertyMetadataDAHInfos);
    result->bwdMetadataDAHInfos = std::move(bwdPropertyMetadataDAHInfos);
    return result;
}

} // namespace storage
} // namespace kuzu

#include "storage/stats/rel_table_statistics.h"

#include "catalog/rel_table_schema.h"
#include "common/serializer/deserializer.h"
#include "common/serializer/serializer.h"
#include "storage/stats/table_statistics_collection.h"
#include "storage/wal/wal.h"

using namespace kuzu::catalog;
using namespace kuzu::common;

namespace kuzu {
namespace storage {

RelTableStats::RelTableStats(
    BMFileHandle* metadataFH, const TableSchema& schema, BufferManager* bufferManager, WAL* wal)
    : TableStatistics{schema}, nextRelOffset{0} {
    const auto& relTableSchema = static_cast<const RelTableSchema&>(schema);
    if (!relTableSchema.isSingleMultiplicityInDirection(RelDataDirection::FWD)) {
        fwdCSROffsetMetadataDAHInfo = TablesStatistics::createMetadataDAHInfo(
            LogicalType{LogicalTypeID::INT64}, *metadataFH, bufferManager, wal);
    }
    if (!relTableSchema.isSingleMultiplicityInDirection(RelDataDirection::BWD)) {
        bwdCSROffsetMetadataDAHInfo = TablesStatistics::createMetadataDAHInfo(
            LogicalType{LogicalTypeID::INT64}, *metadataFH, bufferManager, wal);
    }
    fwdNbrIDMetadataDAHInfo = TablesStatistics::createMetadataDAHInfo(
        LogicalType{LogicalTypeID::INTERNAL_ID}, *metadataFH, bufferManager, wal);
    bwdNbrIDMetadataDAHInfo = TablesStatistics::createMetadataDAHInfo(
        LogicalType{LogicalTypeID::INTERNAL_ID}, *metadataFH, bufferManager, wal);
    fwdPropertyMetadataDAHInfos.clear();
    bwdPropertyMetadataDAHInfos.clear();
    fwdPropertyMetadataDAHInfos.reserve(schema.getNumProperties());
    bwdPropertyMetadataDAHInfos.reserve(schema.getNumProperties());
    for (auto property : schema.getProperties()) {
        fwdPropertyMetadataDAHInfos.push_back(TablesStatistics::createMetadataDAHInfo(
            *property->getDataType(), *metadataFH, bufferManager, wal));
        bwdPropertyMetadataDAHInfos.push_back(TablesStatistics::createMetadataDAHInfo(
            *property->getDataType(), *metadataFH, bufferManager, wal));
    }
}

RelTableStats::RelTableStats(const RelTableStats& other) : TableStatistics{other} {
    nextRelOffset = other.nextRelOffset;
    if (other.fwdCSROffsetMetadataDAHInfo) {
        fwdCSROffsetMetadataDAHInfo = other.fwdCSROffsetMetadataDAHInfo->copy();
    }
    if (other.bwdCSROffsetMetadataDAHInfo) {
        bwdCSROffsetMetadataDAHInfo = other.bwdCSROffsetMetadataDAHInfo->copy();
    }
    fwdNbrIDMetadataDAHInfo = other.fwdNbrIDMetadataDAHInfo->copy();
    bwdNbrIDMetadataDAHInfo = other.bwdNbrIDMetadataDAHInfo->copy();
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
    fwdNbrIDMetadataDAHInfo->serialize(serializer);
    bwdNbrIDMetadataDAHInfo->serialize(serializer);
    serializer.serializeVectorOfPtrs(fwdPropertyMetadataDAHInfos);
    serializer.serializeVectorOfPtrs(bwdPropertyMetadataDAHInfos);
}

std::unique_ptr<RelTableStats> RelTableStats::deserialize(
    uint64_t numRels, table_id_t tableID, Deserializer& deserializer) {
    offset_t nextRelOffset;
    deserializer.deserializeValue(nextRelOffset);
    std::unique_ptr<MetadataDAHInfo> fwdCSROffsetMetadataDAHInfo, bwdCSROffsetMetadataDAHInfo;
    deserializer.deserializeOptionalValue(fwdCSROffsetMetadataDAHInfo);
    deserializer.deserializeOptionalValue(bwdCSROffsetMetadataDAHInfo);
    auto fwdNbrIDMetadataDAHInfo = MetadataDAHInfo::deserialize(deserializer);
    auto bwdNbrIDMetadataDAHInfo = MetadataDAHInfo::deserialize(deserializer);
    std::vector<std::unique_ptr<MetadataDAHInfo>> fwdPropertyMetadataDAHInfos;
    std::vector<std::unique_ptr<MetadataDAHInfo>> bwdPropertyMetadataDAHInfos;
    deserializer.deserializeVectorOfPtrs(fwdPropertyMetadataDAHInfos);
    deserializer.deserializeVectorOfPtrs(bwdPropertyMetadataDAHInfos);
    auto result = std::make_unique<RelTableStats>(numRels, tableID, nextRelOffset);
    result->fwdCSROffsetMetadataDAHInfo = std::move(fwdCSROffsetMetadataDAHInfo);
    result->bwdCSROffsetMetadataDAHInfo = std::move(bwdCSROffsetMetadataDAHInfo);
    result->fwdNbrIDMetadataDAHInfo = std::move(fwdNbrIDMetadataDAHInfo);
    result->bwdNbrIDMetadataDAHInfo = std::move(bwdNbrIDMetadataDAHInfo);
    result->fwdPropertyMetadataDAHInfos = std::move(fwdPropertyMetadataDAHInfos);
    result->bwdPropertyMetadataDAHInfos = std::move(bwdPropertyMetadataDAHInfos);
    return result;
}

} // namespace storage
} // namespace kuzu

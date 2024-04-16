#pragma once

#include "common/enums/rel_direction.h"
#include "storage/buffer_manager/buffer_manager.h"
#include "storage/stats/metadata_dah_info.h"
#include "storage/stats/table_statistics.h"

namespace kuzu {
namespace storage {

class WAL;
class RelTableStats : public TableStatistics {
public:
    RelTableStats(BMFileHandle* metadataFH, const catalog::TableCatalogEntry& tableEntry,
        BufferManager* bufferManager, WAL* wal);
    RelTableStats(uint64_t numRels, common::table_id_t tableID, common::offset_t nextRelOffset)
        : TableStatistics{common::TableType::REL, numRels, tableID, {}},
          nextRelOffset{nextRelOffset} {}
    RelTableStats(uint64_t numRels, common::table_id_t tableID,
        std::unordered_map<common::property_id_t, std::unique_ptr<PropertyStatistics>>&&
            propertyStats,
        common::offset_t nextRelOffset)
        : TableStatistics{common::TableType::REL, numRels, tableID, std::move(propertyStats)},
          nextRelOffset{nextRelOffset} {}

    RelTableStats(const RelTableStats& other);

    common::offset_t getNextRelOffset() const { return nextRelOffset; }
    void incrementNextRelOffset(uint64_t numTuples) { nextRelOffset += numTuples; }

    void addMetadataDAHInfoForColumn(std::unique_ptr<MetadataDAHInfo> metadataDAHInfo,
        common::RelDataDirection direction) {
        auto& metadataDAHInfos = getDirectedMetadataDAHInfosRef(direction);
        metadataDAHInfos.push_back(std::move(metadataDAHInfo));
    }
    void removeMetadataDAHInfoForColumn(common::column_id_t columnID,
        common::RelDataDirection direction) {
        auto& metadataDAHInfos = getDirectedMetadataDAHInfosRef(direction);
        KU_ASSERT(columnID < metadataDAHInfos.size());
        metadataDAHInfos.erase(metadataDAHInfos.begin() + columnID);
    }
    MetadataDAHInfo* getCSROffsetMetadataDAHInfo(common::RelDataDirection direction) {
        return direction == common::RelDataDirection::FWD ? fwdCSROffsetMetadataDAHInfo.get() :
                                                            bwdCSROffsetMetadataDAHInfo.get();
    }
    MetadataDAHInfo* getCSRLengthMetadataDAHInfo(common::RelDataDirection direction) {
        return direction == common::RelDataDirection::FWD ? fwdCSRLengthMetadataDAHInfo.get() :
                                                            bwdCSRLengthMetadataDAHInfo.get();
    }
    MetadataDAHInfo* getColumnMetadataDAHInfo(common::column_id_t columnID,
        common::RelDataDirection direction) {
        auto& metadataDAHInfos = getDirectedMetadataDAHInfosRef(direction);
        KU_ASSERT(columnID < metadataDAHInfos.size());
        return metadataDAHInfos[columnID].get();
    }

    void serializeInternal(common::Serializer& serializer) final;
    static std::unique_ptr<RelTableStats> deserialize(uint64_t numRels, common::table_id_t tableID,
        common::Deserializer& deserializer);

    std::unique_ptr<TableStatistics> copy() final { return std::make_unique<RelTableStats>(*this); }

private:
    std::vector<std::unique_ptr<MetadataDAHInfo>>& getDirectedMetadataDAHInfosRef(
        common::RelDataDirection direction) {
        return direction == common::RelDataDirection::FWD ? fwdMetadataDAHInfos :
                                                            bwdMetadataDAHInfos;
    }

private:
    common::offset_t nextRelOffset;
    // CSROffsetMetadataDAHInfo are only valid for CSRColumns.
    std::unique_ptr<MetadataDAHInfo> fwdCSROffsetMetadataDAHInfo;
    std::unique_ptr<MetadataDAHInfo> bwdCSROffsetMetadataDAHInfo;
    std::unique_ptr<MetadataDAHInfo> fwdCSRLengthMetadataDAHInfo;
    std::unique_ptr<MetadataDAHInfo> bwdCSRLengthMetadataDAHInfo;
    std::vector<std::unique_ptr<MetadataDAHInfo>> fwdMetadataDAHInfos;
    std::vector<std::unique_ptr<MetadataDAHInfo>> bwdMetadataDAHInfos;
};

} // namespace storage
} // namespace kuzu

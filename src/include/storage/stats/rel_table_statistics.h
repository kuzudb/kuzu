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
    RelTableStats(BMFileHandle* metadataFH, const catalog::TableSchema& tableSchema,
        BufferManager* bufferManager, WAL* wal);
    RelTableStats(uint64_t numRels, common::table_id_t tableID, common::offset_t nextRelOffset)
        : TableStatistics{common::TableType::REL, numRels, tableID, {}}, nextRelOffset{
                                                                             nextRelOffset} {}
    RelTableStats(uint64_t numRels, common::table_id_t tableID,
        std::unordered_map<common::property_id_t, std::unique_ptr<PropertyStatistics>>&&
            propertyStats,
        common::offset_t nextRelOffset)
        : TableStatistics{common::TableType::REL, numRels, tableID, std::move(propertyStats)},
          nextRelOffset{nextRelOffset} {}

    RelTableStats(const RelTableStats& other);

    inline common::offset_t getNextRelOffset() const { return nextRelOffset; }
    inline void incrementNextRelOffset(uint64_t numTuples) { nextRelOffset += numTuples; }

    inline void addMetadataDAHInfoForColumn(
        std::unique_ptr<MetadataDAHInfo> metadataDAHInfo, common::RelDataDirection direction) {
        auto& metadataDAHInfos = getDirectedPropertyMetadataDAHInfosRef(direction);
        metadataDAHInfos.push_back(std::move(metadataDAHInfo));
    }
    inline void removeMetadataDAHInfoForColumn(
        common::column_id_t columnID, common::RelDataDirection direction) {
        auto& metadataDAHInfos = getDirectedPropertyMetadataDAHInfosRef(direction);
        KU_ASSERT(columnID < metadataDAHInfos.size());
        metadataDAHInfos.erase(metadataDAHInfos.begin() + columnID);
    }
    inline MetadataDAHInfo* getCSROffsetMetadataDAHInfo(common::RelDataDirection direction) {
        return direction == common::RelDataDirection::FWD ? fwdCSROffsetMetadataDAHInfo.get() :
                                                            bwdCSROffsetMetadataDAHInfo.get();
    }
    inline MetadataDAHInfo* getAdjMetadataDAHInfo(common::RelDataDirection direction) {
        return direction == common::RelDataDirection::FWD ? fwdNbrIDMetadataDAHInfo.get() :
                                                            bwdNbrIDMetadataDAHInfo.get();
    }
    inline MetadataDAHInfo* getPropertyMetadataDAHInfo(
        common::column_id_t columnID, common::RelDataDirection direction) {
        auto& metadataDAHInfos = getDirectedPropertyMetadataDAHInfosRef(direction);
        KU_ASSERT(columnID < metadataDAHInfos.size());
        return metadataDAHInfos[columnID].get();
    }

    void serializeInternal(common::Serializer& serializer) final;
    static std::unique_ptr<RelTableStats> deserialize(
        uint64_t numRels, common::table_id_t tableID, common::Deserializer& deserializer);

    inline std::unique_ptr<TableStatistics> copy() final {
        return std::make_unique<RelTableStats>(*this);
    }

private:
    inline std::vector<std::unique_ptr<MetadataDAHInfo>>& getDirectedPropertyMetadataDAHInfosRef(
        common::RelDataDirection direction) {
        return direction == common::RelDataDirection::FWD ? fwdPropertyMetadataDAHInfos :
                                                            bwdPropertyMetadataDAHInfos;
    }

private:
    common::offset_t nextRelOffset;
    // CSROffsetMetadataDAHInfo are only valid for CSRColumns.
    std::unique_ptr<MetadataDAHInfo> fwdCSROffsetMetadataDAHInfo;
    std::unique_ptr<MetadataDAHInfo> bwdCSROffsetMetadataDAHInfo;
    std::unique_ptr<MetadataDAHInfo> fwdNbrIDMetadataDAHInfo;
    std::unique_ptr<MetadataDAHInfo> bwdNbrIDMetadataDAHInfo;
    std::vector<std::unique_ptr<MetadataDAHInfo>> fwdPropertyMetadataDAHInfos;
    std::vector<std::unique_ptr<MetadataDAHInfo>> bwdPropertyMetadataDAHInfos;
};

} // namespace storage
} // namespace kuzu

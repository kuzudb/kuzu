#pragma once

#include <map>
#include <set>

#include "common/vector/value_vector.h"
#include "storage/stats/metadata_dah_info.h"
#include "storage/stats/table_statistics.h"
#include "storage/storage_structure/disk_array_collection.h"

namespace kuzu {
namespace common {
class Serializer;
class Deserializer;
} // namespace common

namespace storage {

class NodeTableStatsAndDeletedIDs : public TableStatistics {
public:
    NodeTableStatsAndDeletedIDs(DiskArrayCollection& metadataDAC,
        const catalog::TableCatalogEntry& entry);
    NodeTableStatsAndDeletedIDs(common::table_id_t tableID, common::offset_t maxNodeOffset,
        const std::vector<common::offset_t>& deletedNodeOffsets);
    NodeTableStatsAndDeletedIDs(const NodeTableStatsAndDeletedIDs& other);

    common::offset_t getMaxNodeOffset() const {
        return getMaxNodeOffsetFromNumTuples(getNumTuples());
    }

    std::pair<common::offset_t, bool> addNode();

    void deleteNode(common::offset_t nodeOffset);

    // This function assumes nodeIDVector have consecutive node offsets and the same tableID.
    void setDeletedNodeOffsetsForVector(const common::ValueVector* nodeIDVector,
        common::node_group_idx_t nodeGroupIdx, common::idx_t vectorIdxInNodeGroup,
        common::row_idx_t numRowsToScan) const;

    void setNumTuples(uint64_t numTuples) override;

    std::vector<common::offset_t> getDeletedNodeOffsets() const;

    static uint64_t getNumTuplesFromMaxNodeOffset(common::offset_t maxNodeOffset) {
        return (maxNodeOffset == UINT64_MAX) ? 0ull : maxNodeOffset + 1ull;
    }
    static uint64_t getMaxNodeOffsetFromNumTuples(uint64_t numTuples) {
        return numTuples == 0 ? UINT64_MAX : numTuples - 1;
    }

    void addMetadataDAHInfoForColumn(std::unique_ptr<MetadataDAHInfo> metadataDAHInfo) {
        metadataDAHInfos.push_back(std::move(metadataDAHInfo));
    }
    void removeMetadataDAHInfoForColumn(common::column_id_t columnID) {
        KU_ASSERT(columnID < metadataDAHInfos.size());
        metadataDAHInfos.erase(metadataDAHInfos.begin() + columnID);
    }
    MetadataDAHInfo* getMetadataDAHInfo(common::column_id_t columnID) const {
        KU_ASSERT(columnID < metadataDAHInfos.size());
        return metadataDAHInfos[columnID].get();
    }

    void serializeInternal(common::Serializer& serializer) final;
    static std::unique_ptr<NodeTableStatsAndDeletedIDs> deserialize(common::table_id_t tableID,
        common::offset_t maxNodeOffset, common::Deserializer& deserializer);

    std::unique_ptr<TableStatistics> copy() final {
        return std::make_unique<NodeTableStatsAndDeletedIDs>(*this);
    }

private:
    // We pass the morselIdx to not do the division nodeOffset/DEFAULT_VECTOR_CAPACITY again
    bool isDeleted(common::offset_t nodeOffset, uint64_t morselIdx);

private:
    std::vector<std::unique_ptr<MetadataDAHInfo>> metadataDAHInfos;
    std::vector<bool> hasDeletedNodesPerVector;
    std::map<uint64_t, std::set<common::offset_t>> deletedNodeOffsetsPerVector;
};

} // namespace storage
} // namespace kuzu

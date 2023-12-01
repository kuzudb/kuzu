#pragma once

#include <map>
#include <set>

#include "common/vector/value_vector.h"
#include "storage/buffer_manager/buffer_manager.h"
#include "storage/stats/metadata_dah_info.h"
#include "storage/stats/table_statistics.h"
#include "storage/wal/wal.h"

namespace kuzu {
namespace common {
class Serializer;
class Deserializer;
} // namespace common
namespace storage {

class NodeTableStatsAndDeletedIDs : public TableStatistics {
public:
    NodeTableStatsAndDeletedIDs(BMFileHandle* metadataFH, const catalog::TableSchema& schema,
        BufferManager* bufferManager, WAL* wal);
    NodeTableStatsAndDeletedIDs(common::table_id_t tableID, common::offset_t maxNodeOffset,
        const std::vector<common::offset_t>& deletedNodeOffsets);
    NodeTableStatsAndDeletedIDs(common::table_id_t tableID, common::offset_t maxNodeOffset,
        const std::vector<common::offset_t>& deletedNodeOffsets,
        std::unordered_map<common::property_id_t, std::unique_ptr<PropertyStatistics>>&&
            propertyStatistics);
    NodeTableStatsAndDeletedIDs(const NodeTableStatsAndDeletedIDs& other);

    inline common::offset_t getMaxNodeOffset() {
        return getMaxNodeOffsetFromNumTuples(getNumTuples());
    }

    common::offset_t addNode();

    void deleteNode(common::offset_t nodeOffset);

    // This function assumes that it is being called right after ScanNodeID has obtained a
    // morsel and that the nodeID structs in nodeOffsetVector.values have consecutive node
    // offsets and the same tableID.
    void setDeletedNodeOffsetsForMorsel(
        const std::shared_ptr<common::ValueVector>& nodeOffsetVector);

    void setNumTuples(uint64_t numTuples) override;

    std::vector<common::offset_t> getDeletedNodeOffsets() const;

    static inline uint64_t getNumTuplesFromMaxNodeOffset(common::offset_t maxNodeOffset) {
        return (maxNodeOffset == UINT64_MAX) ? 0ull : maxNodeOffset + 1ull;
    }
    static inline uint64_t getMaxNodeOffsetFromNumTuples(uint64_t numTuples) {
        return numTuples == 0 ? UINT64_MAX : numTuples - 1;
    }

    inline void addMetadataDAHInfoForColumn(std::unique_ptr<MetadataDAHInfo> metadataDAHInfo) {
        metadataDAHInfos.push_back(std::move(metadataDAHInfo));
    }
    inline void removeMetadataDAHInfoForColumn(common::column_id_t columnID) {
        KU_ASSERT(columnID < metadataDAHInfos.size());
        metadataDAHInfos.erase(metadataDAHInfos.begin() + columnID);
    }
    inline MetadataDAHInfo* getMetadataDAHInfo(common::column_id_t columnID) {
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
    common::table_id_t tableID;
    std::vector<std::unique_ptr<MetadataDAHInfo>> metadataDAHInfos;
    std::vector<bool> hasDeletedNodesPerMorsel;
    std::map<uint64_t, std::set<common::offset_t>> deletedNodeOffsetsPerMorsel;
};

} // namespace storage
} // namespace kuzu

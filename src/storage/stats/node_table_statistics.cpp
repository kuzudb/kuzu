#include "storage/stats/node_table_statistics.h"

#include "common/serializer/deserializer.h"
#include "common/serializer/serializer.h"
#include "common/string_format.h"
#include "storage/stats/table_statistics_collection.h"
#include "storage/storage_structure/disk_array_collection.h"
#include "storage/storage_utils.h"
#include "storage/wal/wal_record.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

NodeTableStatsAndDeletedIDs::NodeTableStatsAndDeletedIDs(DiskArrayCollection& metadataDAC,
    const catalog::TableCatalogEntry& entry)
    : TableStatistics{entry} {
    metadataDAHInfos.clear();
    metadataDAHInfos.reserve(entry.getNumProperties());
    for (auto& property : entry.getPropertiesRef()) {
        metadataDAHInfos.push_back(
            TablesStatistics::createMetadataDAHInfo(property.getDataType(), metadataDAC));
    }
}

NodeTableStatsAndDeletedIDs::NodeTableStatsAndDeletedIDs(const NodeTableStatsAndDeletedIDs& other)
    : TableStatistics{other}, hasDeletedNodesPerVector{other.hasDeletedNodesPerVector},
      deletedNodeOffsetsPerVector{other.deletedNodeOffsetsPerVector} {
    metadataDAHInfos.clear();
    metadataDAHInfos.reserve(other.metadataDAHInfos.size());
    for (auto& metadataDAHInfo : other.metadataDAHInfos) {
        metadataDAHInfos.push_back(metadataDAHInfo->copy());
    }
}

NodeTableStatsAndDeletedIDs::NodeTableStatsAndDeletedIDs(table_id_t tableID, offset_t maxNodeOffset,
    const std::vector<offset_t>& deletedNodeOffsets)
    : TableStatistics{TableType::NODE, getNumTuplesFromMaxNodeOffset(maxNodeOffset), tableID} {
    if (getNumTuples() > 0) {
        hasDeletedNodesPerVector.resize((getNumTuples() / DEFAULT_VECTOR_CAPACITY) + 1, false);
    }
    for (offset_t deletedNodeOffset : deletedNodeOffsets) {
        auto morselIdxAndOffset =
            StorageUtils::getQuotientRemainder(deletedNodeOffset, DEFAULT_VECTOR_CAPACITY);
        hasDeletedNodesPerVector[morselIdxAndOffset.first] = true;
        if (!deletedNodeOffsetsPerVector.contains(morselIdxAndOffset.first)) {
            deletedNodeOffsetsPerVector.insert({morselIdxAndOffset.first, std::set<offset_t>()});
        }
        deletedNodeOffsetsPerVector.find(morselIdxAndOffset.first)
            ->second.insert(deletedNodeOffset);
    }
}

std::pair<offset_t, bool> NodeTableStatsAndDeletedIDs::addNode() {
    if (deletedNodeOffsetsPerVector.empty()) {
        setNumTuples(getNumTuples() + 1);
        return {getMaxNodeOffset(), true};
    }
    // We return the last element in the first non-empty vector we find.
    const auto iter = deletedNodeOffsetsPerVector.begin();
    auto nodeOffsetIter = iter->second.end();
    --nodeOffsetIter;
    const offset_t retVal = *nodeOffsetIter;
    iter->second.erase(nodeOffsetIter);
    if (iter->second.empty()) {
        hasDeletedNodesPerVector[iter->first] = false;
        deletedNodeOffsetsPerVector.erase(iter);
    }
    return {retVal, false};
}

void NodeTableStatsAndDeletedIDs::deleteNode(offset_t nodeOffset) {
    // TODO(Semih/Guodong): This check can go into nodeOffsetsInfoForWriteTrx->deleteNode
    // once errorIfNodeHasEdges is removed. This function would then just be a wrapper to init
    // nodeOffsetsInfoForWriteTrx before calling delete on it.
    const auto maxNodeOffset = getMaxNodeOffset();
    if (maxNodeOffset == UINT64_MAX || nodeOffset > maxNodeOffset) {
        throw RuntimeException(
            stringFormat("Cannot delete nodeOffset {} in nodeTable {}. maxNodeOffset "
                         "is either -1 or nodeOffset is > maxNodeOffset: {}.",
                nodeOffset, tableID, maxNodeOffset));
    }
    auto morselIdxAndOffset =
        StorageUtils::getQuotientRemainder(nodeOffset, DEFAULT_VECTOR_CAPACITY);
    if (isDeleted(nodeOffset, morselIdxAndOffset.first)) {
        return;
    }
    // TODO(Guodong): Fix delete node with connected edges.
    //    errorIfNodeHasEdges(nodeOffset);
    if (!hasDeletedNodesPerVector[morselIdxAndOffset.first]) {
        std::set<offset_t> deletedNodeOffsets;
        deletedNodeOffsetsPerVector.insert({morselIdxAndOffset.first, deletedNodeOffsets});
    }
    deletedNodeOffsetsPerVector.find(morselIdxAndOffset.first)->second.insert(nodeOffset);
    hasDeletedNodesPerVector[morselIdxAndOffset.first] = true;
}

// Note: this function will always be called right after scanNodeID, so we have the guarantee
// that the nodeOffsetVector is always unselected.
void NodeTableStatsAndDeletedIDs::setDeletedNodeOffsetsForVector(const ValueVector* nodeIDVector,
    node_group_idx_t nodeGroupIdx, idx_t vectorIdxInNodeGroup, row_idx_t numRowsToScan) const {
    const auto vectorIdx =
        nodeGroupIdx * StorageConstants::NUM_VECTORS_PER_NODE_GROUP + vectorIdxInNodeGroup;
    if (hasDeletedNodesPerVector[vectorIdx]) {
        auto& deletedNodeOffsets = deletedNodeOffsetsPerVector.at(vectorIdx);
        const uint64_t vectorBeginOffset = vectorIdx * DEFAULT_VECTOR_CAPACITY;
        auto itr = deletedNodeOffsets.begin();
        sel_t numSelectedValue = 0;
        auto selectedBuffer = nodeIDVector->state->getSelVectorUnsafe().getMultableBuffer();
        KU_ASSERT(nodeIDVector->state->getSelVector().isUnfiltered());
        for (auto pos = 0u; pos < numRowsToScan; ++pos) {
            if (itr == deletedNodeOffsets.end()) { // no more deleted offset to check.
                selectedBuffer[numSelectedValue++] = pos;
                continue;
            }
            if (pos + vectorBeginOffset == *itr) { // node has been deleted.
                ++itr;
                continue;
            }
            selectedBuffer[numSelectedValue++] = pos;
        }
        if (numSelectedValue != numRowsToScan) {
            nodeIDVector->state->getSelVectorUnsafe().setToFiltered();
        }
        nodeIDVector->state->getSelVectorUnsafe().setSelSize(numSelectedValue);
    }
}

void NodeTableStatsAndDeletedIDs::setNumTuples(uint64_t numTuples) {
    TableStatistics::setNumTuples(numTuples);
    if (numTuples > 0) {
        hasDeletedNodesPerVector.resize((numTuples / DEFAULT_VECTOR_CAPACITY) + 1, false);
    }
}

std::vector<offset_t> NodeTableStatsAndDeletedIDs::getDeletedNodeOffsets() const {
    std::vector<offset_t> retVal;
    auto morselIter = deletedNodeOffsetsPerVector.begin();
    while (morselIter != deletedNodeOffsetsPerVector.end()) {
        retVal.insert(retVal.cend(), morselIter->second.begin(), morselIter->second.end());
        ++morselIter;
    }
    return retVal;
}

void NodeTableStatsAndDeletedIDs::serializeInternal(Serializer& serializer) {
    serializer.serializeVector(getDeletedNodeOffsets());
    serializer.serializeVectorOfPtrs(metadataDAHInfos);
}

std::unique_ptr<NodeTableStatsAndDeletedIDs> NodeTableStatsAndDeletedIDs::deserialize(
    table_id_t tableID, offset_t maxNodeOffset, Deserializer& deserializer) {
    std::vector<offset_t> deletedNodeOffsets;
    std::vector<std::unique_ptr<MetadataDAHInfo>> metadataDAHInfos;
    deserializer.deserializeVector(deletedNodeOffsets);
    deserializer.deserializeVectorOfPtrs(metadataDAHInfos);
    auto result =
        std::make_unique<NodeTableStatsAndDeletedIDs>(tableID, maxNodeOffset, deletedNodeOffsets);
    result->metadataDAHInfos = std::move(metadataDAHInfos);
    return result;
}

bool NodeTableStatsAndDeletedIDs::isDeleted(offset_t nodeOffset, uint64_t morselIdx) {
    const auto iter = deletedNodeOffsetsPerVector.find(morselIdx);
    if (iter != deletedNodeOffsetsPerVector.end()) {
        return iter->second.contains(nodeOffset);
    }
    return false;
}

} // namespace storage
} // namespace kuzu

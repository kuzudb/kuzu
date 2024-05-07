#include "storage/stats/node_table_statistics.h"

#include "common/serializer/deserializer.h"
#include "common/serializer/serializer.h"
#include "common/string_format.h"
#include "storage/stats/table_statistics_collection.h"
#include "storage/storage_utils.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

NodeTableStatsAndDeletedIDs::NodeTableStatsAndDeletedIDs(BMFileHandle* metadataFH,
    const catalog::TableCatalogEntry& entry, BufferManager* bufferManager, WAL* wal)
    : TableStatistics{entry}, tableID{entry.getTableID()} {
    metadataDAHInfos.clear();
    metadataDAHInfos.reserve(entry.getNumProperties());
    for (auto& property : entry.getPropertiesRef()) {
        metadataDAHInfos.push_back(TablesStatistics::createMetadataDAHInfo(*property.getDataType(),
            *metadataFH, bufferManager, wal));
    }
}

NodeTableStatsAndDeletedIDs::NodeTableStatsAndDeletedIDs(table_id_t tableID, offset_t maxNodeOffset,
    const std::vector<offset_t>& deletedNodeOffsets)
    : NodeTableStatsAndDeletedIDs{tableID, maxNodeOffset, deletedNodeOffsets, {}} {}

NodeTableStatsAndDeletedIDs::NodeTableStatsAndDeletedIDs(const NodeTableStatsAndDeletedIDs& other)
    : TableStatistics{other}, tableID{other.tableID},
      hasDeletedNodesPerMorsel{other.hasDeletedNodesPerMorsel},
      deletedNodeOffsetsPerMorsel{other.deletedNodeOffsetsPerMorsel} {
    metadataDAHInfos.clear();
    metadataDAHInfos.reserve(other.metadataDAHInfos.size());
    for (auto& metadataDAHInfo : other.metadataDAHInfos) {
        metadataDAHInfos.push_back(metadataDAHInfo->copy());
    }
}

NodeTableStatsAndDeletedIDs::NodeTableStatsAndDeletedIDs(table_id_t tableID, offset_t maxNodeOffset,
    const std::vector<offset_t>& deletedNodeOffsets,
    std::unordered_map<property_id_t, std::unique_ptr<PropertyStatistics>>&& propertyStatistics)
    : TableStatistics{TableType::NODE, getNumTuplesFromMaxNodeOffset(maxNodeOffset), tableID,
          std::move(propertyStatistics)},
      tableID{tableID} {
    if (getNumTuples() > 0) {
        hasDeletedNodesPerMorsel.resize((getNumTuples() / DEFAULT_VECTOR_CAPACITY) + 1, false);
    }
    for (offset_t deletedNodeOffset : deletedNodeOffsets) {
        auto morselIdxAndOffset =
            StorageUtils::getQuotientRemainder(deletedNodeOffset, DEFAULT_VECTOR_CAPACITY);
        hasDeletedNodesPerMorsel[morselIdxAndOffset.first] = true;
        if (!deletedNodeOffsetsPerMorsel.contains(morselIdxAndOffset.first)) {
            deletedNodeOffsetsPerMorsel.insert({morselIdxAndOffset.first, std::set<offset_t>()});
        }
        deletedNodeOffsetsPerMorsel.find(morselIdxAndOffset.first)
            ->second.insert(deletedNodeOffset);
    }
}

offset_t NodeTableStatsAndDeletedIDs::addNode() {
    if (deletedNodeOffsetsPerMorsel.empty()) {
        setNumTuples(getNumTuples() + 1);
        return getMaxNodeOffset();
    }
    // We return the last element in the first non-empty morsel we find
    auto iter = deletedNodeOffsetsPerMorsel.begin();
    auto nodeOffsetIter = iter->second.end();
    nodeOffsetIter--;
    offset_t retVal = *nodeOffsetIter;
    iter->second.erase(nodeOffsetIter);
    if (iter->second.empty()) {
        hasDeletedNodesPerMorsel[iter->first] = false;
        deletedNodeOffsetsPerMorsel.erase(iter);
    }
    return retVal;
}

void NodeTableStatsAndDeletedIDs::deleteNode(offset_t nodeOffset) {
    // TODO(Semih/Guodong): This check can go into nodeOffsetsInfoForWriteTrx->deleteNode
    // once errorIfNodeHasEdges is removed. This function would then just be a wrapper to init
    // nodeOffsetsInfoForWriteTrx before calling delete on it.
    auto maxNodeOffset = getMaxNodeOffset();
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
    if (!hasDeletedNodesPerMorsel[morselIdxAndOffset.first]) {
        std::set<offset_t> deletedNodeOffsets;
        deletedNodeOffsetsPerMorsel.insert({morselIdxAndOffset.first, deletedNodeOffsets});
    }
    deletedNodeOffsetsPerMorsel.find(morselIdxAndOffset.first)->second.insert(nodeOffset);
    hasDeletedNodesPerMorsel[morselIdxAndOffset.first] = true;
}

// Note: this function will always be called right after scanNodeID, so we have the guarantee
// that the nodeOffsetVector is always unselected.
void NodeTableStatsAndDeletedIDs::setDeletedNodeOffsetsForMorsel(ValueVector* nodeIDVector) const {
    auto [morselIdx, _] = StorageUtils::getQuotientRemainder(nodeIDVector->readNodeOffset(0),
        DEFAULT_VECTOR_CAPACITY);
    if (hasDeletedNodesPerMorsel[morselIdx]) {
        auto& deletedNodeOffsets = deletedNodeOffsetsPerMorsel.at(morselIdx);
        uint64_t morselBeginOffset = morselIdx * DEFAULT_VECTOR_CAPACITY;
        auto originalSize = nodeIDVector->state->getOriginalSize();
        auto itr = deletedNodeOffsets.begin();
        common::sel_t numSelectedValue = 0;
        auto selectedBuffer = nodeIDVector->state->getSelVectorUnsafe().getMultableBuffer();
        KU_ASSERT(nodeIDVector->state->getSelVector().isUnfiltered());
        for (auto pos = 0u; pos < nodeIDVector->state->getOriginalSize(); ++pos) {
            if (itr == deletedNodeOffsets.end()) { // no more deleted offset to check.
                selectedBuffer[numSelectedValue++] = pos;
                continue;
            }
            if (pos + morselBeginOffset == *itr) { // node has been deleted.
                itr++;
                continue;
            }
            selectedBuffer[numSelectedValue++] = pos;
        }
        if (numSelectedValue != originalSize) {
            nodeIDVector->state->getSelVectorUnsafe().setToFiltered();
        }
        nodeIDVector->state->getSelVectorUnsafe().setSelSize(numSelectedValue);
    }
}

void NodeTableStatsAndDeletedIDs::setNumTuples(uint64_t numTuples) {
    TableStatistics::setNumTuples(numTuples);
    if (numTuples > 0) {
        hasDeletedNodesPerMorsel.resize((numTuples / DEFAULT_VECTOR_CAPACITY) + 1, false);
    }
}

std::vector<offset_t> NodeTableStatsAndDeletedIDs::getDeletedNodeOffsets() const {
    std::vector<offset_t> retVal;
    auto morselIter = deletedNodeOffsetsPerMorsel.begin();
    while (morselIter != deletedNodeOffsetsPerMorsel.end()) {
        retVal.insert(retVal.cend(), morselIter->second.begin(), morselIter->second.end());
        morselIter++;
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
    auto iter = deletedNodeOffsetsPerMorsel.find(morselIdx);
    if (iter != deletedNodeOffsetsPerMorsel.end()) {
        return iter->second.contains(nodeOffset);
    }
    return false;
}

} // namespace storage
} // namespace kuzu

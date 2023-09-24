#include "storage/stats/node_table_statistics.h"

#include "common/ser_deser.h"
#include "common/string_utils.h"
#include "storage/stats/table_statistics_collection.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

NodeTableStatsAndDeletedIDs::NodeTableStatsAndDeletedIDs(BMFileHandle* metadataFH,
    const catalog::TableSchema& schema, BufferManager* bufferManager, WAL* wal)
    : TableStatistics{schema}, tableID{schema.tableID} {
    metadataDAHInfos.clear();
    metadataDAHInfos.reserve(schema.getNumProperties());
    for (auto property : schema.getProperties()) {
        metadataDAHInfos.push_back(TablesStatistics::createMetadataDAHInfo(
            *property->getDataType(), *metadataFH, bufferManager, wal));
    }
}

NodeTableStatsAndDeletedIDs::NodeTableStatsAndDeletedIDs(
    table_id_t tableID, offset_t maxNodeOffset, const std::vector<offset_t>& deletedNodeOffsets)
    : NodeTableStatsAndDeletedIDs{tableID, maxNodeOffset, deletedNodeOffsets, {}} {}

NodeTableStatsAndDeletedIDs::NodeTableStatsAndDeletedIDs(const NodeTableStatsAndDeletedIDs& other)
    : TableStatistics{other}, tableID{other.tableID}, adjListsAndColumns{other.adjListsAndColumns},
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
    : tableID{tableID}, TableStatistics{TableType::NODE,
                            getNumTuplesFromMaxNodeOffset(maxNodeOffset), tableID,
                            std::move(propertyStatistics)} {
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
            StringUtils::string_format("Cannot delete nodeOffset {} in nodeTable {}. maxNodeOffset "
                                       "is either -1 or nodeOffset is > maxNodeOffset: {}.",
                nodeOffset, tableID, maxNodeOffset));
    }
    auto morselIdxAndOffset =
        StorageUtils::getQuotientRemainder(nodeOffset, DEFAULT_VECTOR_CAPACITY);
    if (isDeleted(nodeOffset, morselIdxAndOffset.first)) {
        throw RuntimeException(
            StringUtils::string_format("Node with offset {} is already deleted.", nodeOffset));
    }
    errorIfNodeHasEdges(nodeOffset);
    if (!hasDeletedNodesPerMorsel[morselIdxAndOffset.first]) {
        std::set<offset_t> deletedNodeOffsets;
        deletedNodeOffsetsPerMorsel.insert({morselIdxAndOffset.first, deletedNodeOffsets});
    }
    deletedNodeOffsetsPerMorsel.find(morselIdxAndOffset.first)->second.insert(nodeOffset);
    hasDeletedNodesPerMorsel[morselIdxAndOffset.first] = true;
}

// Note: this function will always be called right after scanNodeID, so we have the guarantee
// that the nodeOffsetVector is always unselected.
void NodeTableStatsAndDeletedIDs::setDeletedNodeOffsetsForMorsel(
    const std::shared_ptr<ValueVector>& nodeOffsetVector) {
    auto morselIdxAndOffset = StorageUtils::getQuotientRemainder(
        nodeOffsetVector->readNodeOffset(0), DEFAULT_VECTOR_CAPACITY);
    if (hasDeletedNodesPerMorsel[morselIdxAndOffset.first]) {
        auto deletedNodeOffsets = deletedNodeOffsetsPerMorsel[morselIdxAndOffset.first];
        uint64_t morselBeginOffset = morselIdxAndOffset.first * DEFAULT_VECTOR_CAPACITY;
        nodeOffsetVector->state->selVector->resetSelectorToValuePosBuffer();
        auto itr = deletedNodeOffsets.begin();
        sel_t nextDeletedNodeOffset = *itr - morselBeginOffset;
        uint64_t nextSelectedPosition = 0;
        for (sel_t pos = 0; pos < nodeOffsetVector->state->getOriginalSize(); ++pos) {
            if (pos == nextDeletedNodeOffset) {
                itr++;
                if (itr == deletedNodeOffsets.end()) {
                    nextDeletedNodeOffset = UINT16_MAX;
                    // We do not break because we need to keep setting the positions after
                    // the last deletedNodeOffset.
                    continue;
                }
                nextDeletedNodeOffset = *itr - morselBeginOffset;
                continue;
            }
            nodeOffsetVector->state->selVector->selectedPositions[nextSelectedPosition++] = pos;
        }
        nodeOffsetVector->state->selVector->selectedSize =
            nodeOffsetVector->state->getOriginalSize() - deletedNodeOffsets.size();
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

void NodeTableStatsAndDeletedIDs::serializeInternal(FileInfo* fileInfo, uint64_t& offset) {
    SerDeser::serializeVector(getDeletedNodeOffsets(), fileInfo, offset);
    SerDeser::serializeVectorOfPtrs(metadataDAHInfos, fileInfo, offset);
}

std::unique_ptr<NodeTableStatsAndDeletedIDs> NodeTableStatsAndDeletedIDs::deserialize(
    table_id_t tableID, offset_t maxNodeOffset, FileInfo* fileInfo, uint64_t& offset) {
    std::vector<offset_t> deletedNodeOffsets;
    std::vector<std::unique_ptr<MetadataDAHInfo>> metadataDAHInfos;
    SerDeser::deserializeVector(deletedNodeOffsets, fileInfo, offset);
    SerDeser::deserializeVectorOfPtrs(metadataDAHInfos, fileInfo, offset);
    auto result =
        std::make_unique<NodeTableStatsAndDeletedIDs>(tableID, maxNodeOffset, deletedNodeOffsets);
    result->metadataDAHInfos = std::move(metadataDAHInfos);
    return result;
}

void NodeTableStatsAndDeletedIDs::errorIfNodeHasEdges(offset_t nodeOffset) {
    for (AdjLists* adjList : adjListsAndColumns.first) {
        auto numElementsInList =
            adjList->getTotalNumElementsInList(transaction::TransactionType::WRITE, nodeOffset);
        if (numElementsInList != 0) {
            throw RuntimeException(StringUtils::string_format(
                "Currently deleting a node with edges is not supported. node table {} nodeOffset "
                "{} has {} (one-to-many or many-to-many) edges.",
                tableID, nodeOffset, numElementsInList));
        }
    }
    for (Column* adjColumn : adjListsAndColumns.second) {
        if (!adjColumn->isNull(nodeOffset, transaction::Transaction::getDummyWriteTrx().get())) {
            throw RuntimeException(StringUtils::string_format(
                "Currently deleting a node with edges is not supported. node table {} nodeOffset "
                "{}  has a 1-1 edge.",
                tableID, nodeOffset));
        }
    }
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

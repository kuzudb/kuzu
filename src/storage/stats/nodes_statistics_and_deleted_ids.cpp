#include "storage/stats/nodes_statistics_and_deleted_ids.h"

#include "common/constants.h"
#include "common/string_utils.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

NodeTableStatsAndDeletedIDs::NodeTableStatsAndDeletedIDs(table_id_t tableID, offset_t maxNodeOffset,
    const std::vector<offset_t>& deletedNodeOffsets,
    std::unordered_map<common::property_id_t, std::unique_ptr<PropertyStatistics>>&&
        propertyStatistics)
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

std::vector<offset_t> NodeTableStatsAndDeletedIDs::getDeletedNodeOffsets() {
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
}

std::unique_ptr<NodeTableStatsAndDeletedIDs> NodeTableStatsAndDeletedIDs::deserialize(
    table_id_t tableID, offset_t maxNodeOffset, FileInfo* fileInfo, uint64_t& offset) {
    std::vector<common::offset_t> deletedNodeOffsets;
    SerDeser::deserializeVector(deletedNodeOffsets, fileInfo, offset);
    return std::make_unique<NodeTableStatsAndDeletedIDs>(
        tableID, maxNodeOffset, deletedNodeOffsets);
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

NodesStatisticsAndDeletedIDs::NodesStatisticsAndDeletedIDs(
    std::unordered_map<table_id_t, std::unique_ptr<NodeTableStatsAndDeletedIDs>>&
        nodesStatisticsAndDeletedIDs)
    : TablesStatistics{} {
    initTableStatisticsForWriteTrx();
    for (auto& nodeStatistics : nodesStatisticsAndDeletedIDs) {
        tablesStatisticsContentForReadOnlyTrx->tableStatisticPerTable[nodeStatistics.first] =
            std::make_unique<NodeTableStatsAndDeletedIDs>(
                *(NodeTableStatsAndDeletedIDs*)nodeStatistics.second.get());
        tablesStatisticsContentForWriteTrx->tableStatisticPerTable[nodeStatistics.first] =
            std::make_unique<NodeTableStatsAndDeletedIDs>(
                *(NodeTableStatsAndDeletedIDs*)nodeStatistics.second.get());
    }
}

void NodesStatisticsAndDeletedIDs::setAdjListsAndColumns(RelsStore* relsStore) {
    for (auto& tableIDStatistics : tablesStatisticsContentForReadOnlyTrx->tableStatisticPerTable) {
        getNodeStatisticsAndDeletedIDs(tableIDStatistics.first)
            ->setAdjListsAndColumns(relsStore->getAdjListsAndColumns(tableIDStatistics.first));
    }
}

std::map<table_id_t, offset_t> NodesStatisticsAndDeletedIDs::getMaxNodeOffsetPerTable() const {
    std::map<table_id_t, offset_t> retVal;
    for (auto& tableIDStatistics : tablesStatisticsContentForReadOnlyTrx->tableStatisticPerTable) {
        retVal[tableIDStatistics.first] =
            getNodeStatisticsAndDeletedIDs(tableIDStatistics.first)->getMaxNodeOffset();
    }
    return retVal;
}

void NodesStatisticsAndDeletedIDs::setDeletedNodeOffsetsForMorsel(
    transaction::Transaction* transaction, const std::shared_ptr<ValueVector>& nodeOffsetVector,
    table_id_t tableID) {
    // NOTE: We can remove the lock under the following assumptions, that should currently hold:
    // 1) During the phases when nodeStatisticsAndDeletedIDsPerTableForReadOnlyTrx change, which
    // is during checkpointing, this function, which is called during scans, cannot be called.
    // 2) In a read-only transaction, the same morsel cannot be scanned concurrently. 3) A
    // write transaction cannot have two concurrent pipelines where one is writing and the
    // other is reading nodeStatisticsAndDeletedIDsPerTableForWriteTrx. That is the pipeline in a
    // query where scans/reads happen in a write transaction cannot run concurrently with the
    // pipeline that performs an add/delete node.
    lock_t lck{mtx};
    (transaction->isReadOnly() || tablesStatisticsContentForWriteTrx == nullptr) ?
        getNodeStatisticsAndDeletedIDs(tableID)->setDeletedNodeOffsetsForMorsel(nodeOffsetVector) :
        ((NodeTableStatsAndDeletedIDs*)tablesStatisticsContentForWriteTrx
                ->tableStatisticPerTable[tableID]
                .get())
            ->setDeletedNodeOffsetsForMorsel(nodeOffsetVector);
}

void NodesStatisticsAndDeletedIDs::addNodeStatisticsAndDeletedIDs(
    catalog::NodeTableSchema* tableSchema) {
    initTableStatisticsForWriteTrx();
    tablesStatisticsContentForWriteTrx->tableStatisticPerTable[tableSchema->tableID] =
        constructTableStatistic(tableSchema);
}

} // namespace storage
} // namespace kuzu

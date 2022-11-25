#include "storage/store/nodes_statistics_and_deleted_ids.h"

#include "common/configs.h"
#include "spdlog/spdlog.h"

using namespace std;

namespace kuzu {
namespace storage {

NodeStatisticsAndDeletedIDs::NodeStatisticsAndDeletedIDs(table_id_t tableID,
    node_offset_t maxNodeOffset, const vector<node_offset_t>& deletedNodeOffsets)
    : tableID{tableID} {
    auto numTuples = geNumTuplesFromMaxNodeOffset(maxNodeOffset);
    TableStatistics::setNumTuples(numTuples);
    if (numTuples > 0) {
        hasDeletedNodesPerMorsel.resize((numTuples / DEFAULT_VECTOR_CAPACITY) + 1, false);
    }
    for (node_offset_t deletedNodeOffset : deletedNodeOffsets) {
        auto morselIdxAndOffset =
            StorageUtils::getQuotientRemainder(deletedNodeOffset, DEFAULT_VECTOR_CAPACITY);
        hasDeletedNodesPerMorsel[morselIdxAndOffset.first] = true;
        if (!deletedNodeOffsetsPerMorsel.contains(morselIdxAndOffset.first)) {
            deletedNodeOffsetsPerMorsel.insert({morselIdxAndOffset.first, set<node_offset_t>()});
        }
        deletedNodeOffsetsPerMorsel.find(morselIdxAndOffset.first)
            ->second.insert(deletedNodeOffset);
    }
}

node_offset_t NodeStatisticsAndDeletedIDs::addNode() {
    if (deletedNodeOffsetsPerMorsel.empty()) {
        setNumTuples(getNumTuples() + 1);
        return getMaxNodeOffset();
    }
    // We return the last element in the first non-empty morsel we find
    auto iter = deletedNodeOffsetsPerMorsel.begin();
    set<node_offset_t> deletedNodeOffsets = iter->second;
    auto nodeOffsetIter = iter->second.end();
    nodeOffsetIter--;
    node_offset_t retVal = *nodeOffsetIter;
    iter->second.erase(nodeOffsetIter);
    if (iter->second.empty()) {
        deletedNodeOffsetsPerMorsel.erase(iter);
        hasDeletedNodesPerMorsel[iter->first] = false;
    }
    return retVal;
}

void NodeStatisticsAndDeletedIDs::deleteNode(node_offset_t nodeOffset) {
    // TODO(Semih/Guodong): This check can go into nodeOffsetsInfoForWriteTrx->deleteNode
    // once errorIfNodeHasEdges is removed. This function would then just be a wrapper to init
    // nodeOffsetsInfoForWriteTrx before calling delete on it.
    auto maxNodeOffset = getMaxNodeOffset();
    if (maxNodeOffset == UINT64_MAX || nodeOffset > maxNodeOffset) {
        throw RuntimeException(
            StringUtils::string_format("Cannot delete nodeOffset %d in nodeTable %d. maxNodeOffset "
                                       "is either -1 or nodeOffset is > maxNodeOffset: %d.",
                nodeOffset, tableID, maxNodeOffset));
    }
    auto morselIdxAndOffset =
        StorageUtils::getQuotientRemainder(nodeOffset, DEFAULT_VECTOR_CAPACITY);
    if (isDeleted(nodeOffset, morselIdxAndOffset.first)) {
        throw RuntimeException(
            StringUtils::string_format("Node with offset %d is already deleted.", nodeOffset));
    }
    errorIfNodeHasEdges(nodeOffset);
    if (!hasDeletedNodesPerMorsel[morselIdxAndOffset.first]) {
        set<node_offset_t> deletedNodeOffsets;
        deletedNodeOffsetsPerMorsel.insert({morselIdxAndOffset.first, deletedNodeOffsets});
    }
    deletedNodeOffsetsPerMorsel.find(morselIdxAndOffset.first)->second.insert(nodeOffset);
    hasDeletedNodesPerMorsel[morselIdxAndOffset.first] = true;
}

void NodeStatisticsAndDeletedIDs::setDeletedNodeOffsetsForMorsel(
    const shared_ptr<ValueVector>& nodeOffsetVector) {
    auto morselIdxAndOffset = StorageUtils::getQuotientRemainder(
        nodeOffsetVector->readNodeOffset(0), DEFAULT_VECTOR_CAPACITY);

    if (hasDeletedNodesPerMorsel[morselIdxAndOffset.first]) {
        auto deletedNodeOffsets = deletedNodeOffsetsPerMorsel[morselIdxAndOffset.first];
        uint64_t morselBeginOffset = morselIdxAndOffset.first * DEFAULT_VECTOR_CAPACITY;
        nodeOffsetVector->state->selVector->resetSelectorToValuePosBuffer();
        auto itr = deletedNodeOffsets.begin();
        sel_t nextDeletedNodeOffset = *itr - morselBeginOffset;
        uint64_t nextSelectedPosition = 0;
        for (sel_t pos = 0; pos < nodeOffsetVector->state->originalSize; ++pos) {
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
            nodeOffsetVector->state->originalSize - deletedNodeOffsets.size();
    }
}

void NodeStatisticsAndDeletedIDs::setNumTuples(uint64_t numTuples) {
    TableStatistics::setNumTuples(numTuples);
    if (numTuples > 0) {
        hasDeletedNodesPerMorsel.resize((numTuples / DEFAULT_VECTOR_CAPACITY) + 1, false);
    }
}

vector<node_offset_t> NodeStatisticsAndDeletedIDs::getDeletedNodeOffsets() {
    vector<node_offset_t> retVal;
    auto morselIter = deletedNodeOffsetsPerMorsel.begin();
    while (morselIter != deletedNodeOffsetsPerMorsel.end()) {
        retVal.insert(retVal.cend(), morselIter->second.begin(), morselIter->second.end());
        morselIter++;
    }
    return retVal;
}

void NodeStatisticsAndDeletedIDs::errorIfNodeHasEdges(node_offset_t nodeOffset) {
    for (AdjLists* adjList : adjListsAndColumns.first) {
        auto numElementsInList =
            adjList->getTotalNumElementsInList(TransactionType::WRITE, nodeOffset);
        if (numElementsInList != 0) {
            throw RuntimeException(StringUtils::string_format(
                "Currently deleting a node with edges is not supported. node table %d nodeOffset "
                "%d has %d (one-to-many or many-to-many) edges for edge file: %s.",
                tableID, nodeOffset, numElementsInList,
                adjList->getFileHandle()->getFileInfo()->path.c_str()));
        }
    }
    for (AdjColumn* adjColumn : adjListsAndColumns.second) {
        if (!adjColumn->isNull(nodeOffset, Transaction::getDummyWriteTrx().get())) {
            throw RuntimeException(StringUtils::string_format(
                "Currently deleting a node with edges is not supported. node table %d nodeOffset "
                "%d  has a 1-1 edge for edge file: %s.",
                tableID, nodeOffset, adjColumn->getFileHandle()->getFileInfo()->path.c_str()));
        }
    }
}

bool NodeStatisticsAndDeletedIDs::isDeleted(node_offset_t nodeOffset, uint64_t morselIdx) {
    auto iter = deletedNodeOffsetsPerMorsel.find(morselIdx);
    if (iter != deletedNodeOffsetsPerMorsel.end()) {
        return iter->second.contains(nodeOffset);
    }
    return false;
}

NodesStatisticsAndDeletedIDs::NodesStatisticsAndDeletedIDs(
    unordered_map<table_id_t, unique_ptr<NodeStatisticsAndDeletedIDs>>&
        nodesStatisticsAndDeletedIDs)
    : TablesStatistics{} {
    initTableStatisticPerTableForWriteTrxIfNecessary();
    for (auto& nodeStatistics : nodesStatisticsAndDeletedIDs) {
        tablesStatisticsContentForReadOnlyTrx->tableStatisticPerTable[nodeStatistics.first] =
            make_unique<NodeStatisticsAndDeletedIDs>(
                *(NodeStatisticsAndDeletedIDs*)nodeStatistics.second.get());
        tablesStatisticsContentForWriteTrx->tableStatisticPerTable[nodeStatistics.first] =
            make_unique<NodeStatisticsAndDeletedIDs>(
                *(NodeStatisticsAndDeletedIDs*)nodeStatistics.second.get());
    }
}

void NodesStatisticsAndDeletedIDs::setAdjListsAndColumns(RelsStore* relsStore) {
    for (auto& tableIDStatistics : tablesStatisticsContentForReadOnlyTrx->tableStatisticPerTable) {
        getNodeStatisticsAndDeletedIDs(tableIDStatistics.first)
            ->setAdjListsAndColumns(relsStore->getAdjListsAndColumns(tableIDStatistics.first));
    }
}

map<table_id_t, node_offset_t> NodesStatisticsAndDeletedIDs::getMaxNodeOffsetPerTable() const {
    map<table_id_t, node_offset_t> retVal;
    for (auto& tableIDStatistics : tablesStatisticsContentForReadOnlyTrx->tableStatisticPerTable) {
        retVal[tableIDStatistics.first] =
            getNodeStatisticsAndDeletedIDs(tableIDStatistics.first)->getMaxNodeOffset();
    }
    return retVal;
}

void NodesStatisticsAndDeletedIDs::setDeletedNodeOffsetsForMorsel(
    Transaction* transaction, const shared_ptr<ValueVector>& nodeOffsetVector, table_id_t tableID) {
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
        ((NodeStatisticsAndDeletedIDs*)tablesStatisticsContentForWriteTrx
                ->tableStatisticPerTable[tableID]
                .get())
            ->setDeletedNodeOffsetsForMorsel(nodeOffsetVector);
}

void NodesStatisticsAndDeletedIDs::addNodeStatisticsAndDeletedIDs(NodeTableSchema* tableSchema) {
    initTableStatisticPerTableForWriteTrxIfNecessary();
    // We use UINT64_MAX to represent an empty nodeTable which doesn't contain any nodes.
    tablesStatisticsContentForWriteTrx->tableStatisticPerTable[tableSchema->tableID] =
        make_unique<NodeStatisticsAndDeletedIDs>(
            tableSchema->tableID, UINT64_MAX /* maxNodeOffset */);
}

unique_ptr<TableStatistics> NodesStatisticsAndDeletedIDs::deserializeTableStatistics(
    uint64_t numTuples, uint64_t& offset, FileInfo* fileInfo, uint64_t tableID) {
    vector<node_offset_t> deletedNodeIDs;
    offset = SerDeser::deserializeVector(deletedNodeIDs, fileInfo, offset);
    return make_unique<NodeStatisticsAndDeletedIDs>(tableID,
        NodeStatisticsAndDeletedIDs::getMaxNodeOffsetFromNumTuples(numTuples), deletedNodeIDs);
}

void NodesStatisticsAndDeletedIDs::serializeTableStatistics(
    TableStatistics* tableStatistics, uint64_t& offset, FileInfo* fileInfo) {
    auto nodeTableStatistic = (NodeStatisticsAndDeletedIDs*)tableStatistics;
    offset =
        SerDeser::serializeVector(nodeTableStatistic->getDeletedNodeOffsets(), fileInfo, offset);
}

} // namespace storage
} // namespace kuzu

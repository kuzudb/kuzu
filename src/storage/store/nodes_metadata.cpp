#include "src/storage/store/include/nodes_metadata.h"

#include "spdlog/spdlog.h"

#include "src/common/include/configs.h"

using namespace std;

namespace graphflow {
namespace storage {

NodeOffsetsInfo::NodeOffsetsInfo(
    node_offset_t maxNodeOffset_, const vector<node_offset_t>& deletedNodeOffsets_) {
    setMaxNodeOffset(maxNodeOffset_);
    for (node_offset_t deletedNodeOffset : deletedNodeOffsets_) {
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

NodeOffsetsInfo::NodeOffsetsInfo(NodeOffsetsInfo& other) {
    maxNodeOffset = other.maxNodeOffset;
    hasDeletedNodesPerMorsel = other.hasDeletedNodesPerMorsel;
    deletedNodeOffsetsPerMorsel = other.deletedNodeOffsetsPerMorsel;
}

void NodeOffsetsInfo::setMaxNodeOffset(node_offset_t maxNodeOffset_) {
    maxNodeOffset = maxNodeOffset_;
    if (maxNodeOffset != UINT64_MAX) {
        hasDeletedNodesPerMorsel.resize((maxNodeOffset / DEFAULT_VECTOR_CAPACITY) + 1, false);
    }
}

// This function assumes that it is being called right after ScanNodeID has obtained a morsel
// and that the nodeID structs in nodeOffsetVector.values have consecutive node offsets and
// the same table.
void NodeOffsetsInfo::setDeletedNodeOffsetsForMorsel(
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

node_offset_t NodeOffsetsInfo::addNode() {
    if (deletedNodeOffsetsPerMorsel.empty()) {
        maxNodeOffset++;
        return maxNodeOffset;
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

void NodeOffsetsInfo::deleteNode(node_offset_t nodeOffset) {
    auto morselIdxAndOffset =
        StorageUtils::getQuotientRemainder(nodeOffset, DEFAULT_VECTOR_CAPACITY);
    if (isDeleted(nodeOffset, morselIdxAndOffset.first)) {
        throw RuntimeException(
            StringUtils::string_format("Node with offset %d is already deleted.", nodeOffset));
    }

    if (!hasDeletedNodesPerMorsel[morselIdxAndOffset.first]) {
        set<node_offset_t> deletedNodeOffsets;
        deletedNodeOffsetsPerMorsel.insert({morselIdxAndOffset.first, deletedNodeOffsets});
    }
    deletedNodeOffsetsPerMorsel.find(morselIdxAndOffset.first)->second.insert(nodeOffset);
    hasDeletedNodesPerMorsel[morselIdxAndOffset.first] = true;
}

vector<node_offset_t> NodeOffsetsInfo::getDeletedNodeOffsets() {
    vector<node_offset_t> retVal;
    auto morselIter = deletedNodeOffsetsPerMorsel.begin();
    while (morselIter != deletedNodeOffsetsPerMorsel.end()) {
        retVal.insert(retVal.cend(), morselIter->second.begin(), morselIter->second.end());
        morselIter++;
    }
    return retVal;
}

// We pass the morselIdx to not do the division nodeOffset/DEFAULT_VECTOR_CAPACITY again
bool NodeOffsetsInfo::isDeleted(node_offset_t nodeOffset, uint64_t morselIdx) {
    auto iter = deletedNodeOffsetsPerMorsel.find(morselIdx);
    if (iter != deletedNodeOffsetsPerMorsel.end()) {
        return iter->second.contains(nodeOffset);
    }
    return false;
}

NodeMetadata::NodeMetadata(table_id_t tableID, node_offset_t maxNodeOffset,
    const vector<node_offset_t>& deletedNodeOffsets)
    : tableID{tableID} {
    nodeOffsetsInfo = make_unique<NodeOffsetsInfo>(maxNodeOffset, deletedNodeOffsets);
}

void NodeMetadata::errorIfNodeHasEdges(node_offset_t nodeOffset) {
    for (AdjLists* adjList : adjListsAndColumns.first) {
        auto listInfo = adjList->getListInfo(nodeOffset);
        if (!listInfo.isEmpty()) {
            throw RuntimeException(StringUtils::string_format(
                "Currently deleting a node with edges is not supported. node table %d nodeOffset "
                "%d has %d (one-to-many or many-to-many) edges for edge file: %s.",
                tableID, nodeOffset, listInfo.listLen,
                adjList->getFileHandle()->getFileInfo()->path.c_str()));
        }
    }
    for (AdjColumn* adjColumn : adjListsAndColumns.second) {
        if (!adjColumn->isNull(nodeOffset)) {
            throw RuntimeException(StringUtils::string_format(
                "Currently deleting a node with edges is not supported. node table %d nodeOffset "
                "%d  has a 1-1 edge for edge file: %s.",
                tableID, nodeOffset, adjColumn->getFileHandle()->getFileInfo()->path.c_str()));
        }
    }
}

// See the comment for addNode().
void NodeMetadata::deleteNode(node_offset_t nodeOffset) {
    // TODO(Semih/Guodong): This check can go into nodeOffsetsInfoForWriteTrx->deleteNode
    // once errorIfNodeHasEdges is removed. This function would then just be a wrapper to init
    // nodeOffsetsInfoForWriteTrx before calling delete on it.
    if (nodeOffsetsInfo->maxNodeOffset == UINT64_MAX ||
        nodeOffset > nodeOffsetsInfo->maxNodeOffset) {
        throw RuntimeException(
            StringUtils::string_format("Cannot delete nodeOffset %d in nodeTable %d. maxNodeOffset "
                                       "is either -1 or nodeOffset is > maxNodeOffset: %d.",
                nodeOffset, tableID, nodeOffsetsInfo->maxNodeOffset));
    }
    errorIfNodeHasEdges(nodeOffset);
    nodeOffsetsInfo->deleteNode(nodeOffset);
}

NodesMetadata::NodesMetadata() {
    logger = LoggerUtils::getOrCreateSpdLogger("storage");
    logger->info("Initializing NodesMetadata.");
    nodeMetadataPerTableForReadOnlyTrx = make_unique<vector<unique_ptr<NodeMetadata>>>();
    logger->info("Initialized NodesMetadata.");
}

NodesMetadata::NodesMetadata(const string& directory) {
    logger = LoggerUtils::getOrCreateSpdLogger("storage");
    logger->info("Initializing NodesMetadata.");
    nodeMetadataPerTableForReadOnlyTrx = make_unique<vector<unique_ptr<NodeMetadata>>>();
    readFromFile(directory);
    logger->info("Initialized NodesMetadata.");
}

NodesMetadata::NodesMetadata(vector<unique_ptr<NodeMetadata>>& nodeMetadataPerTable_) {
    nodeMetadataPerTableForReadOnlyTrx = make_unique<vector<unique_ptr<NodeMetadata>>>();
    for (auto& nodeMetadata : nodeMetadataPerTable_) {
        nodeMetadataPerTableForReadOnlyTrx->push_back(move(nodeMetadata));
    }
}

void NodesMetadata::writeNodesMetadataFileForWALRecord(const string& directory) {
    saveToFile(directory, DBFileType::WAL_VERSION, TransactionType::WRITE);
}

void NodesMetadata::readFromFile(const string& directory) {
    auto nodesMetadataPath =
        StorageUtils::getNodesMetadataFilePath(directory, DBFileType::ORIGINAL);
    auto fileInfo = FileUtils::openFile(nodesMetadataPath, O_RDONLY);
    logger->info("Reading NodesMetadata from {}.", nodesMetadataPath);
    uint64_t offset = 0;
    uint64_t numNodeTables;
    offset = SerDeser::deserializeValue<uint64_t>(numNodeTables, fileInfo.get(), offset);
    for (auto tableID = 0u; tableID < numNodeTables; tableID++) {
        node_offset_t maxNodeOffset;
        offset = SerDeser::deserializeValue<node_offset_t>(maxNodeOffset, fileInfo.get(), offset);
        vector<node_offset_t> deletedNodeIDs;
        offset = SerDeser::deserializeVector(deletedNodeIDs, fileInfo.get(), offset);
        nodeMetadataPerTableForReadOnlyTrx->push_back(
            make_unique<NodeMetadata>(tableID, maxNodeOffset, deletedNodeIDs));
    }
    FileUtils::closeFile(fileInfo->fd);
}

void NodesMetadata::setAdjListsAndColumns(RelsStore* relsStore) {
    for (auto tableID = 0u; tableID < nodeMetadataPerTableForReadOnlyTrx->size(); ++tableID) {
        (*nodeMetadataPerTableForReadOnlyTrx)[tableID]->setAdjListsAndColumns(
            relsStore->getAdjListsAndColumns(tableID));
    }
}

void NodesMetadata::saveToFile(
    const string& directory, DBFileType dbFileType, TransactionType transactionType) {
    auto nodesMetadataPath = StorageUtils::getNodesMetadataFilePath(directory, dbFileType);
    logger->info("Writing NodesMetadata to {}.", nodesMetadataPath);
    auto fileInfo = FileUtils::openFile(nodesMetadataPath, O_WRONLY | O_CREAT);

    uint64_t offset = 0;
    auto& nodeMetadataPerTable = (transactionType == TransactionType::READ_ONLY ||
                                     nodeMetadataPerTableForWriteTrx == nullptr) ?
                                     nodeMetadataPerTableForReadOnlyTrx :
                                     nodeMetadataPerTableForWriteTrx;
    offset = SerDeser::serializeValue(nodeMetadataPerTable->size(), fileInfo.get(), offset);
    for (auto tableID = 0; tableID < nodeMetadataPerTable->size(); ++tableID) {
        auto& nodeMetadata = (*nodeMetadataPerTable)[tableID];
        offset = SerDeser::serializeValue(nodeMetadata->getMaxNodeOffset(), fileInfo.get(), offset);
        offset = SerDeser::serializeVector(
            nodeMetadata->getDeletedNodeOffsets(), fileInfo.get(), offset);
    }
    FileUtils::closeFile(fileInfo->fd);
    logger->info("Wrote NodesMetadata to {}.", nodesMetadataPath);
}

void NodesMetadata::initNodeMetadataPerTableForWriteTrxIfNecessaryNoLock() {
    if (!nodeMetadataPerTableForWriteTrx) {
        nodeMetadataPerTableForWriteTrx = make_unique<vector<unique_ptr<NodeMetadata>>>();
        for (auto& nodeMetadata : *nodeMetadataPerTableForReadOnlyTrx) {
            nodeMetadataPerTableForWriteTrx->push_back(make_unique<NodeMetadata>(*nodeMetadata));
        }
    }
}

vector<node_offset_t> NodesMetadata::getMaxNodeOffsetPerTable() const {
    vector<node_offset_t> retVal;
    for (table_id_t tableID = 0u; tableID < nodeMetadataPerTableForReadOnlyTrx->size(); ++tableID) {
        retVal.push_back((*nodeMetadataPerTableForReadOnlyTrx)[tableID]->getMaxNodeOffset());
    }
    return retVal;
}

void NodesMetadata::setDeletedNodeOffsetsForMorsel(
    Transaction* transaction, const shared_ptr<ValueVector>& nodeOffsetVector, table_id_t tableID) {
    // NOTE: We can remove the lock under the following assumptions, that should currently hold:
    // 1) During the phases when nodeMetadataPerTableForReadOnlyTrx change, which is during
    // checkpointing, this function, which is called during scans, cannot be called.
    // 2) In a read-only transaction, the same morsel cannot be scanned concurrently.
    // 3) A write transaction cannot have two concurrent pipelines where one is writing and the
    // other is reading nodeMetadataPerTableForWriteTrx. That is the pipeline in a query where
    // scans/reads happen in a write transaction cannot run concurrently with the pipeline that
    // performs an add/delete node.
    lock_t lck{mtx};
    (transaction->isReadOnly() || !nodeMetadataPerTableForWriteTrx) ?
        (*nodeMetadataPerTableForReadOnlyTrx)[tableID]->setDeletedNodeOffsetsForMorsel(
            nodeOffsetVector) :
        (*nodeMetadataPerTableForWriteTrx)[tableID]->setDeletedNodeOffsetsForMorsel(
            nodeOffsetVector);
}

void NodesMetadata::addNodeMetadata(NodeTableSchema* tableSchema) {
    initNodeMetadataPerTableForWriteTrxIfNecessaryNoLock();
    // We use UINT64_MAX to represent an empty nodeTable which doesn't contain any nodes.
    nodeMetadataPerTableForWriteTrx->push_back(
        make_unique<NodeMetadata>(tableSchema->tableID, UINT64_MAX /* maxNodeOffset */));
}

} // namespace storage
} // namespace graphflow

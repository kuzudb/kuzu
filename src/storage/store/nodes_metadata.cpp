#include "src/storage/store/include/nodes_metadata.h"

#include "spdlog/spdlog.h"

#include "src/common/include/configs.h"

using namespace std;

namespace graphflow {
namespace storage {

NodeOffsetsInfo::NodeOffsetsInfo(
    node_offset_t maxNodeOffset, vector<node_offset_t> deletedNodeOffsets_)
    : maxNodeOffset{maxNodeOffset} {
    hasDeletedNodesPerMorsel.resize((maxNodeOffset / DEFAULT_VECTOR_CAPACITY) + 1, false);
    for (node_offset_t deletedNodeOffset : deletedNodeOffsets_) {
        auto morselIdxAndOffset =
            StorageUtils::getQuotientRemainder(deletedNodeOffset, DEFAULT_VECTOR_CAPACITY);
        hasDeletedNodesPerMorsel[morselIdxAndOffset.first] = true;
        set<node_offset_t> sortedDeletedNodeOffsetsForMorsel;
        if (deletedNodeOffsetsPerMorsel.contains(morselIdxAndOffset.first)) {
            // This should copy the vector to a new variable.
            sortedDeletedNodeOffsetsForMorsel =
                deletedNodeOffsetsPerMorsel.find(morselIdxAndOffset.first)->second;
        } else {
            deletedNodeOffsetsPerMorsel.insert(
                {morselIdxAndOffset.first, sortedDeletedNodeOffsetsForMorsel});
        }
        sortedDeletedNodeOffsetsForMorsel.insert(deletedNodeOffset);
    }
}

NodeOffsetsInfo::NodeOffsetsInfo(NodeOffsetsInfo& other) {
    maxNodeOffset = other.maxNodeOffset;
    hasDeletedNodesPerMorsel = other.hasDeletedNodesPerMorsel;
    deletedNodeOffsetsPerMorsel = other.deletedNodeOffsetsPerMorsel;
}

// This function assumes that it is being called right after ScanNodeID has obtained a morsel
// and that the nodeID structs in nodeOffsetVector.values have consecutive node offsets and
// the same labels.
void NodeOffsetsInfo::setDeletedNodeOffsetsForMorsel(shared_ptr<ValueVector> nodeOffsetVector) {
    auto morselIdxAndOffset = StorageUtils::getQuotientRemainder(
        nodeOffsetVector->readNodeOffset(0), DEFAULT_VECTOR_CAPACITY);
    cout << "Setting deleted node offsets for morsel: nodeOffsetVector->readNodeOffset(0): "
         << nodeOffsetVector->readNodeOffset(0) << endl;

    if (hasDeletedNodesPerMorsel[morselIdxAndOffset.first]) {
        auto deletedNodeOffsets = deletedNodeOffsetsPerMorsel[morselIdxAndOffset.first];
        uint64_t morselBeginOffset = morselIdxAndOffset.first * DEFAULT_VECTOR_CAPACITY;
        cout << "Setting " << deletedNodeOffsets.size() << " many deleted nodes." << endl;
        cout << " nodeOffsetVector->state->originalSize:  " << nodeOffsetVector->state->originalSize
             << endl;
        nodeOffsetVector->state->resetSelectorToValuePosBuffer();
        auto itr = deletedNodeOffsets.begin();
        sel_t nextDeletedNodeOffset = *itr - morselBeginOffset;
        cout << "deletedNodeOffsets first element: " << *itr << "- morselBeginOffset"
             << morselBeginOffset << " = nextDeletedNodeOffset: " << nextDeletedNodeOffset << endl;
        uint64_t nextSelectedPosition = 0;
        for (sel_t pos = 0; pos < nodeOffsetVector->state->originalSize; ++pos) {
            if (pos == nextDeletedNodeOffset) {
                itr++;
                if (itr == deletedNodeOffsets.end()) {
                    cout << "Reached deletedNodeOffsets.end(). No more deletedNodeOffsets in "
                            "this morsel."
                         << endl;
                    // TODO(Reviewer): I wanted to define a SEL_T_MAX in types.h right under
                    // where typedef uint16_t sel_t; line. But then I get a linking error:
                    // duplicate symbol 'graphflow::common::SEL_T_MAX' in:
                    // bazel-out/darwin-fastbuild/bin/src/storage/_objs/hash_index/hash_index.o
                    // bazel-out/darwin-fastbuild/bin/src/storage/_objs/hash_index/hash_index_utils.o
                    nextDeletedNodeOffset = UINT16_MAX;
                    // We do not break because we need to keep setting the positions after
                    // the last deletedNodeOffset.
                    continue;
                }
                nextDeletedNodeOffset = *itr - morselBeginOffset;
                cout << "Incrementing the iterator. nextDeletedNodeOffset: "
                     << nextDeletedNodeOffset << endl;
                continue;
            }
            nodeOffsetVector->state->selectedPositions[nextSelectedPosition++] = pos;
        }
        nodeOffsetVector->state->setSelectedSize(
            nodeOffsetVector->state->originalSize - deletedNodeOffsets.size());
        cout << "Finished setting setDeletedNodeOffsetsForMorsel. "
                "nodeOffsetVector->state->setSelectedSize: "
             << nodeOffsetVector->state->selectedSize << endl;
    }
}

node_offset_t NodeOffsetsInfo::addNode() {
    cout << "Add node is called" << endl;
    if (deletedNodeOffsetsPerMorsel.empty()) {
        maxNodeOffset++;
        cout << "returning nodeOffset from maxNodeOffset " << maxNodeOffset << endl;
        return maxNodeOffset;
    }
    cout << "deletedNodeOffsetsPerMorsel is NOT empty." << endl;
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
    cout << "Returning nodeOffset: " << retVal << endl;
    return retVal;
}

void NodeOffsetsInfo::deleteNode(node_offset_t nodeOffset) {
    auto morselIdxAndOffset =
        StorageUtils::getQuotientRemainder(nodeOffset, DEFAULT_VECTOR_CAPACITY);
    if (isDeleted(nodeOffset, morselIdxAndOffset.first)) {
        throw RuntimeException(
            StringUtils::string_format("Node with offset %d is already deleted: ", nodeOffset));
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

NodeMetadata::NodeMetadata(
    label_t labelID, node_offset_t maxNodeOffset, vector<node_offset_t> deletedNodeOffsets)
    : labelID{labelID} {
    nodeOffsetsInfoForReadOnlyTrx = make_unique<NodeOffsetsInfo>(maxNodeOffset, deletedNodeOffsets);
}
void NodeMetadata::initMaxAndDeletedNodeOffsetsForWriteTrxIfNecessaryNoLock() {
    if (!nodeOffsetsInfoForWriteTrx) {
        nodeOffsetsInfoForWriteTrx = make_unique<NodeOffsetsInfo>(*nodeOffsetsInfoForReadOnlyTrx);
        *nodeOffsetsInfoForWriteTrx = *nodeOffsetsInfoForReadOnlyTrx;
    }
}

void NodeMetadata::errorIfNodeHasEdges(node_offset_t nodeOffset) {
    for (AdjLists* adjList : adjListsAndColumns.first) {
        auto listInfo = adjList->getListInfo(nodeOffset);
        if (!listInfo.isEmpty()) {
            throw RuntimeException(StringUtils::string_format(
                "Currently deleting a node with edges is not supported. node label %d nodeOffset "
                "%d has %d (one-to-many or many-to-many) edges for edge file: %s.",
                labelID, nodeOffset, listInfo.listLen,
                adjList->getFileHandle()->getFileInfo()->path.c_str()));
        }
    }
    for (AdjColumn* adjColumn : adjListsAndColumns.second) {
        if (!adjColumn->isNull(nodeOffset)) {
            throw RuntimeException(StringUtils::string_format(
                "Currently deleting a node with edges is not supported. node label %d nodeOffset "
                "%d  has a 1-1 edge for edge file: %s.",
                labelID, nodeOffset, adjColumn->getFileHandle()->getFileInfo()->path.c_str()));
        }
    }
}

// This function assumes that there is a single write transaction. That is why for now we
// keep the interface simple and no transaction is passed.
node_offset_t NodeMetadata::addNode() {
    lock_t lck{mtx};
    initMaxAndDeletedNodeOffsetsForWriteTrxIfNecessaryNoLock();
    return nodeOffsetsInfoForWriteTrx->addNode();
}

// See the comment for addNode().
void NodeMetadata::deleteNode(node_offset_t nodeOffset) {
    lock_t lck{mtx};
    initMaxAndDeletedNodeOffsetsForWriteTrxIfNecessaryNoLock();
    // TODO(Semih/Guodong): This check can go into nodeOffsetsInfoForWriteTrx->deleteNode
    // once errorIfNodeHasEdges is removed. This function would then just be a wrapper to init
    // nodeOffsetsInfoForWriteTrx before calling delete on it
    if (nodeOffset > nodeOffsetsInfoForWriteTrx->maxNodeOffset) {
        throw RuntimeException(
            StringUtils::string_format("Cannot delete nodeOffset %d in nodeLabel %d. Node "
                                       "offset is > maxNodeOffset: %d.",
                nodeOffset, labelID, nodeOffsetsInfoForWriteTrx->maxNodeOffset));
    }
    errorIfNodeHasEdges(nodeOffset);
    nodeOffsetsInfoForWriteTrx->deleteNode(nodeOffset);
}

// This function assumes that it is being called right after ScanNodeID has obtained a morsel
// and that the nodeID structs in nodeOffsetVector.values have consecutive node offsets and
// the same labels.
void NodeMetadata::setDeletedNodeOffsetsForMorsel(
    Transaction* transaction, shared_ptr<ValueVector> nodeOffsetVector) {
    // TODO: We can remove the below lock under the following
    // assumptions that should currently hold:
    // 1) During the phases when nodeOffsetsInfoForReadOnlyTrx change, which is during
    // checkpionting, this function cannot be called, which is during scans.
    // 2) In a read-only transaction, the same morsel cannot be scanned concurrently.
    // 3) A write transaction cannot have a concurrent pipeline that is updating
    // nodeOffsetsInfoForWriteTrx and also using it during scans. That is the pipeline in a query
    // where scans/reads happen in a write transaction cannot run concurrently with the pipeline
    // that performs a delete.
    lock_t lck{mtx};
    NodeOffsetsInfo* nodeOffsetsInfo = (transaction->isReadOnly() || !nodeOffsetsInfoForWriteTrx) ?
                                           nodeOffsetsInfoForReadOnlyTrx.get() :
                                           nodeOffsetsInfoForWriteTrx.get();
    nodeOffsetsInfo->setDeletedNodeOffsetsForMorsel(nodeOffsetVector);
}

void NodeMetadata::rollbackIfNecessary() {
    lock_t lck{mtx};
    nodeOffsetsInfoForWriteTrx.reset();
}

void NodeMetadata::commitIfNecessary() {
    lock_t lck{mtx};
    cout << "Starting checkpointing of nodes_metadata in memory" << endl;
    if (!hasUpdates()) {
        cout << "Skipping checkpointing because nodes_metadata has no updates" << endl;
        return;
    }
    nodeOffsetsInfoForReadOnlyTrx = make_unique<NodeOffsetsInfo>(*nodeOffsetsInfoForWriteTrx);
    cout << "Finished checkpointing of nodes_metadata in memory" << endl;
}

NodesMetadata::NodesMetadata(const string& directory) {
    logger = LoggerUtils::getOrCreateSpdLogger("storage");
    logger->info("Initializing NodesMetadata.");
    readFromFile(directory);
    logger->info("Initializing NodesMetadata done.");
}

bool NodesMetadata::hasUpdates() {
    for (label_t label = 0u; label < nodeMetadataPerLabel.size(); ++label) {
        if (nodeMetadataPerLabel[label]->hasUpdates()) {
            return true;
        }
    }
    return false;
}

void NodesMetadata::commitIfNecessary() {
    for (label_t label = 0u; label < nodeMetadataPerLabel.size(); ++label) {
        nodeMetadataPerLabel[label]->commitIfNecessary();
    }
}

void NodesMetadata::rollbackIfNecessary() {
    for (label_t label = 0u; label < nodeMetadataPerLabel.size(); ++label) {
        nodeMetadataPerLabel[label]->rollbackIfNecessary();
    }
}

void NodesMetadata::saveToFile(
    const string& directory, vector<node_offset_t>& maxNodeOffsetsPerLabel) {
    vector<vector<node_offset_t>> deletedNodes;
    for (int i = 0; i < maxNodeOffsetsPerLabel.size(); ++i) {
        deletedNodes.push_back(vector<node_offset_t>());
    }
    NodesMetadata::saveToFile(
        directory, false /* is not for wal record */, maxNodeOffsetsPerLabel, deletedNodes);
}

void NodesMetadata::saveToFile(const string& directory, bool isForWALRecord,
    vector<node_offset_t>& maxNodeOffsetsPerLabel,
    vector<vector<node_offset_t>>& deletedNodeOffsetsPerLabel) {
    auto nodesMetadataPath = StorageUtils::getNodesMetadataFilePath(directory, isForWALRecord);
    auto fileInfo = FileUtils::openFile(nodesMetadataPath, O_WRONLY | O_CREAT);
    cout << "Writing NodesMetadata to " << nodesMetadataPath
         << (isForWALRecord ? " for wal record" : "") << endl;

    uint64_t offset = 0;
    offset = SerDeser::serializeValue(maxNodeOffsetsPerLabel.size(), fileInfo.get(), offset);
    for (label_t label = 0; label < deletedNodeOffsetsPerLabel.size(); ++label) {
        offset = SerDeser::serializeValue(maxNodeOffsetsPerLabel[label], fileInfo.get(), offset);
        offset =
            SerDeser::serializeVector(deletedNodeOffsetsPerLabel[label], fileInfo.get(), offset);
    }

    FileUtils::closeFile(fileInfo->fd);
    cout << "Wrote NodesMetadata to " << nodesMetadataPath
         << (isForWALRecord ? " for wal record" : "") << endl;
}

void NodesMetadata::writeNodesMetadataWALFile(const string& directory) {
    vector<node_offset_t> maxNodeOffsetPerLabel;
    vector<vector<node_offset_t>> deletedNodeOffsetsPerLabel;
    for (label_t label = 0; label < nodeMetadataPerLabel.size(); ++label) {
        maxNodeOffsetPerLabel.push_back(
            nodeMetadataPerLabel[label]->getMaxNodeOffset(false /* for write trx */));
        cout << "label: " << label << ". Pushed back maxNodeOffset: "
             << nodeMetadataPerLabel[label]->getMaxNodeOffset(false /* for write trx */) << endl;
        deletedNodeOffsetsPerLabel.push_back(
            nodeMetadataPerLabel[label]->getDeletedNodeOffsetsForWriteTrx());
    }
    NodesMetadata::saveToFile(
        directory, true /* is for wal record */, maxNodeOffsetPerLabel, deletedNodeOffsetsPerLabel);
}

void NodesMetadata::readFromFile(const string& directory) {
    auto nodesMetadataPath =
        FileUtils::joinPath(directory, common::StorageConfig::NODES_METADATA_FILE_NAME);
    auto fileInfo = FileUtils::openFile(nodesMetadataPath, O_RDONLY);
    logger->debug("Reading NodesMetadata from {}.", nodesMetadataPath);

    uint64_t offset = 0;
    uint64_t numNodeLabels;
    offset = SerDeser::deserializeValue<uint64_t>(numNodeLabels, fileInfo.get(), offset);
    cout << "numNodeLabels: " << numNodeLabels << endl;
    for (auto labelID = 0u; labelID < numNodeLabels; labelID++) {
        cout << "Reading labelID: " << labelID << " offset: " << offset << endl;
        node_offset_t maxNodeOffset;
        offset = SerDeser::deserializeValue<node_offset_t>(maxNodeOffset, fileInfo.get(), offset);
        cout << "Read maxNodeOffset: " << maxNodeOffset << " offset: " << offset << endl;
        vector<node_offset_t> sortedDeletedNodeIDs;
        offset = SerDeser::deserializeVector(sortedDeletedNodeIDs, fileInfo.get(), offset);
        cout << "Read sortedDeletedNodeIDs. offset: " << offset << endl;
        nodeMetadataPerLabel.push_back(
            make_unique<NodeMetadata>(labelID, maxNodeOffset, sortedDeletedNodeIDs));
    }
    FileUtils::closeFile(fileInfo->fd);
}

} // namespace storage
} // namespace graphflow

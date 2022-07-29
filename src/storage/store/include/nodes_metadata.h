#pragma once

#include <map>
#include <set>

#include "src/common/include/ser_deser.h"
#include "src/storage/store/include/rels_store.h"

namespace graphflow {
namespace storage {

using lock_t = unique_lock<mutex>;
class NodeMetadata;
// Note: NodeOffsetsInfo is not thread-safe.
class NodeOffsetsInfo {
    friend class NodeMetadata;
    friend class NodesMetadata;

public:
    NodeOffsetsInfo(node_offset_t maxNodeOffset, const vector<node_offset_t>& deletedNodeOffsets_);
    NodeOffsetsInfo(NodeOffsetsInfo& other);

private:
    // This function assumes that it is being called right after ScanNodeID has obtained a morsel
    // and that the nodeID structs in nodeOffsetVector.values have consecutive node offsets and
    // the same labels.
    void setDeletedNodeOffsetsForMorsel(const shared_ptr<ValueVector>& nodeOffsetVector);

    node_offset_t addNode();

    void deleteNode(node_offset_t nodeOffset);

    vector<node_offset_t> getDeletedNodeOffsets();

    // We pass the morselIdx to not do the division nodeOffset/DEFAULT_VECTOR_CAPACITY again
    bool isDeleted(node_offset_t nodeOffset, uint64_t morselIdx);

private:
    vector<bool> hasDeletedNodesPerMorsel;
    map<uint64_t, set<node_offset_t>> deletedNodeOffsetsPerMorsel;
    node_offset_t maxNodeOffset;
};

class NodesMetadata;
class NodeMetadata {
    friend class NodesMetadata;

public:
    NodeMetadata(label_t labelID, node_offset_t maxNodeOffset)
        : NodeMetadata(labelID, maxNodeOffset,
              vector<node_offset_t>() /* no deleted node offsets during initial loading */) {}

    NodeMetadata(label_t labelID, node_offset_t maxNodeOffset,
        const vector<node_offset_t>& deletedNodeOffsets);
    // This function assumes that there is a single write transaction. That is why for now we
    // keep the interface simple and no transaction is passed.

    NodeMetadata(const NodeMetadata& other)
        : labelID{other.labelID}, adjListsAndColumns{other.adjListsAndColumns} {
        nodeOffsetsInfo = make_unique<NodeOffsetsInfo>(*other.nodeOffsetsInfo);
    }

    // See the comment for addNode().
    void deleteNode(node_offset_t nodeOffset);

    // This function assumes that it is being called right after ScanNodeID has obtained a morsel
    // and that the nodeID structs in nodeOffsetVector.values have consecutive node offsets and
    // the same labels.
    inline void setDeletedNodeOffsetsForMorsel(const shared_ptr<ValueVector>& nodeOffsetVector) {
        nodeOffsetsInfo->setDeletedNodeOffsetsForMorsel(nodeOffsetVector);
    }

    inline node_offset_t getMaxNodeOffset() { return nodeOffsetsInfo->maxNodeOffset; }
    inline void setAdjListsAndColumns(
        pair<vector<AdjLists*>, vector<AdjColumn*>> adjListsAndColumns_) {
        adjListsAndColumns = adjListsAndColumns_;
    }

private:
    void errorIfNodeHasEdges(node_offset_t nodeOffset);

    inline vector<node_offset_t> getDeletedNodeOffsetsForWriteTrx() {
        return nodeOffsetsInfo->getDeletedNodeOffsets();
    }

private:
    label_t labelID;
    // Note: This is initialized explicitly through a call to setAdjListsAndColumns after
    // construction.
    pair<vector<AdjLists*>, vector<AdjColumn*>> adjListsAndColumns;
    unique_ptr<NodeOffsetsInfo> nodeOffsetsInfo;
};

// Manages the disk image of the maxNodeOffsets and deleted node IDs (per node label).
// Note: This class is *not* thread-safe.
class NodesMetadata {

public:
    // Should be used when an already loaded database is started from a directory.
    NodesMetadata(const string& directory);

    // Should be used ony by tests;
    explicit NodesMetadata(vector<unique_ptr<NodeMetadata>>& nodeMetadataPerLabel_);

    inline NodeMetadata* getNodeMetadata(label_t label) const {
        return (*nodeMetadataPerLabelForReadOnlyTrx)[label].get();
    }

    void writeNodesMetadataFileForWALRecord(const string& directory);

    inline bool hasUpdates() { return nodeMetadataPerLabelForWriteTrx != nullptr; }

    inline void checkpointInMemoryIfNecessary() {
        lock_t lck{mtx};
        nodeMetadataPerLabelForReadOnlyTrx = move(nodeMetadataPerLabelForWriteTrx);
    }

    inline void rollbackInMemoryIfNecessary() {
        lock_t lck{mtx};
        nodeMetadataPerLabelForWriteTrx.reset();
    }

    void readFromFile(const string& directory);

    void setAdjListsAndColumns(RelsStore* relsStore);

    // Should be used by the loader to write an initial NodesMetadata with no deleted nodes.
    static void saveToFile(const string& directory, vector<node_offset_t>& maxNodeOffsetsPerLabel,
        shared_ptr<spdlog::logger>& logger);

    static void saveToFile(const string& directory, bool isForWALRecord,
        vector<node_offset_t>& maxNodeOffsetsPerLabel,
        vector<vector<uint64_t>>& deletedNodeOffsetsPerLabel, shared_ptr<spdlog::logger>& logger);

    void initNodeMetadataPerLabelForWriteTrxIfNecessaryNoLock();

    // This function assumes that there is a single write transaction. That is why for now we
    // keep the interface simple and no transaction is passed.
    node_offset_t addNode(label_t labelID) {
        lock_t lck{mtx};
        initNodeMetadataPerLabelForWriteTrxIfNecessaryNoLock();
        return (*nodeMetadataPerLabelForWriteTrx)[labelID]->nodeOffsetsInfo->addNode();
    }

    // Refer to the comments for addNode.
    void deleteNode(label_t labelID, node_offset_t nodeOffset) {
        lock_t lck{mtx};
        initNodeMetadataPerLabelForWriteTrxIfNecessaryNoLock();
        (*nodeMetadataPerLabelForWriteTrx)[labelID]->deleteNode(nodeOffset);
    }

    inline node_offset_t getMaxNodeOffset(Transaction* transaction, label_t labelID) {
        return getMaxNodeOffset(transaction == nullptr || transaction->isReadOnly() ?
                                    TransactionType::READ_ONLY :
                                    TransactionType::WRITE,
            labelID);
    }

    inline node_offset_t getMaxNodeOffset(TransactionType transactionType, label_t labelID) {
        return (transactionType == TransactionType::READ_ONLY ||
                   nodeMetadataPerLabelForWriteTrx == nullptr) ?
                   (*nodeMetadataPerLabelForReadOnlyTrx)[labelID]->getMaxNodeOffset() :
                   (*nodeMetadataPerLabelForWriteTrx)[labelID]->getMaxNodeOffset();
    }

    // This function is only used by storageManager to construct relsStore during start-up, so we
    // can just safely return the maxNodeOffsetPerLabel for readOnlyVersion.
    vector<node_offset_t> getMaxNodeOffsetPerLabel();

    void setDeletedNodeOffsetsForMorsel(
        Transaction* transaction, const shared_ptr<ValueVector>& nodeOffsetVector, label_t labelID);

    void addNodeMetadata(NodeLabel* nodeLabel);

    // This function is only used for testing purpose.
    inline uint32_t getNumNodeMetadataPerLabel() const {
        return nodeMetadataPerLabelForReadOnlyTrx->size();
    }

private:
    mutex mtx;
    shared_ptr<spdlog::logger> logger;
    string directory;
    unique_ptr<vector<unique_ptr<NodeMetadata>>> nodeMetadataPerLabelForReadOnlyTrx;
    unique_ptr<vector<unique_ptr<NodeMetadata>>> nodeMetadataPerLabelForWriteTrx;
};

} // namespace storage
} // namespace graphflow

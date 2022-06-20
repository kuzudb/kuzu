#pragma once

#include <map>
#include <set>

#include "src/common/include/ser_deser.h"
#include "src/storage/include/storage_utils.h"
#include "src/storage/storage_structure/include/column.h"
#include "src/storage/storage_structure/include/lists/lists.h"
#include "src/storage/store/include/rels_store.h"

using namespace std;

namespace graphflow {
namespace storage {

using lock_t = unique_lock<mutex>;
class NodeMetadata;
// Note: NodeOffsetsInfo is not thread-safe.
class NodeOffsetsInfo {
    friend class NodeMetadata;

public:
    NodeOffsetsInfo(node_offset_t maxNodeOffset, vector<node_offset_t> deletedNodeOffsets_);
    NodeOffsetsInfo(NodeOffsetsInfo& other);

private:
    // This function assumes that it is being called right after ScanNodeID has obtained a morsel
    // and that the nodeID structs in nodeOffsetVector.values have consecutive node offsets and
    // the same labels.
    void setDeletedNodeOffsetsForMorsel(shared_ptr<ValueVector> nodeOffsetVector);

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
    // Currently used by plan_test_helper.h.
    NodeMetadata(label_t labelID, node_offset_t maxNodeOffset)
        : NodeMetadata(labelID, maxNodeOffset,
              vector<node_offset_t>() /* no deleted node offsets during initial loading */) {}

    NodeMetadata(
        label_t labelID, node_offset_t maxNodeOffset, vector<node_offset_t> deletedNodeOffsets);
    // This function assumes that there is a single write transaction. That is why for now we
    // keep the interface simple and no transaction is passed.
    node_offset_t addNode();

    // See the comment for addNode().
    void deleteNode(node_offset_t nodeOffset);

    // This function assumes that it is being called right after ScanNodeID has obtained a morsel
    // and that the nodeID structs in nodeOffsetVector.values have consecutive node offsets and
    // the same labels.
    void setDeletedNodeOffsetsForMorsel(
        Transaction* transaction, shared_ptr<ValueVector> nodeOffsetVector);

    void commitIfNecessary();

    void rollbackIfNecessary();

    inline bool hasUpdates() { return nodeOffsetsInfoForWriteTrx != nullptr; }

    inline label_t getLabelID() { return labelID; }

    inline node_offset_t getMaxNodeOffset(Transaction* transaction) {
        return getMaxNodeOffset(
            (transaction == nullptr || transaction->isReadOnly()) ? true : false);
    }

    // Helper method to return the maxNodeOffset for read only trxs for parts of system that
    // don't have access to a transaction.
    inline node_offset_t getMaxNodeOffset() { return getMaxNodeOffset(nullptr); }

    inline node_offset_t getMaxNodeOffset(bool isReadOnly) {
        return (isReadOnly || !nodeOffsetsInfoForWriteTrx) ?
                   nodeOffsetsInfoForReadOnlyTrx->maxNodeOffset :
                   nodeOffsetsInfoForWriteTrx->maxNodeOffset;
    }
    inline void setAdjListsAndColumns(
        pair<vector<AdjLists*>, vector<AdjColumn*>> adjListsAndColumns_) {
        adjListsAndColumns = adjListsAndColumns_;
    }

private:
    void errorIfNodeHasEdges(node_offset_t nodeOffset);

    void initMaxAndDeletedNodeOffsetsForWriteTrxIfNecessaryNoLock();

    inline vector<node_offset_t> getDeletedNodeOffsetsForWriteTrx() {
        NodeOffsetsInfo* nodeOffsetsInfo = nodeOffsetsInfoForWriteTrx ?
                                               nodeOffsetsInfoForWriteTrx.get() :
                                               nodeOffsetsInfoForReadOnlyTrx.get();
        return nodeOffsetsInfo->getDeletedNodeOffsets();
    }

private:
    mutex mtx;
    label_t labelID;
    // Note: This is initialized explicitly through a call to setAdjListsAndColumns after
    // construction.
    pair<vector<AdjLists*>, vector<AdjColumn*>> adjListsAndColumns;
    unique_ptr<NodeOffsetsInfo> nodeOffsetsInfoForReadOnlyTrx;
    unique_ptr<NodeOffsetsInfo> nodeOffsetsInfoForWriteTrx;
};

// Manages the disk image of the maxNodeOffsets and deleted node IDs (per node label).
// Note: This class is *not* thread-safe.
class NodesMetadata {

public:
    // Should be used when an already loaded database is started from a directory.
    NodesMetadata(const string& directory);

    // Should be used ony by tests;
    NodesMetadata(vector<unique_ptr<NodeMetadata>>& nodeMetadataPerLabel_) {
        for (int i = 0; i < nodeMetadataPerLabel_.size(); ++i) {
            nodeMetadataPerLabel.push_back(move(nodeMetadataPerLabel_[i]));
        }
    }

    NodeMetadata* getNodeMetadata(label_t label) const { return nodeMetadataPerLabel[label].get(); }

    vector<node_offset_t> getMaxNodeOffsetPerLabel() {
        vector<node_offset_t> retVal;
        for (label_t label = 0u; label < nodeMetadataPerLabel.size(); ++label) {
            retVal.push_back(nodeMetadataPerLabel[label]->getMaxNodeOffset());
        }
        return retVal;
    }

    void writeNodesMetadataFileForWALRecord(const string& directory);

    bool hasUpdates();

    void commitIfNecessary();

    void rollbackIfNecessary();

    void readFromFile(const string& directory);

    void setAdjListsAndColumns(RelsStore* relsStore) {
        for (label_t nodeLabel = 0u; nodeLabel < nodeMetadataPerLabel.size(); ++nodeLabel) {
            nodeMetadataPerLabel[nodeLabel]->setAdjListsAndColumns(
                relsStore->getAdjListsAndColumns(nodeLabel));
        }
    }

    // Should be used by the loader to write an initial NodesMetadata with no deleted nodes.
    static void saveToFile(const string& directory, vector<node_offset_t>& maxNodeOffsetsPerLabel,
        shared_ptr<spdlog::logger>& logger);

    static void saveToFile(const string& directory, bool isForWALRecord,
        vector<node_offset_t>& maxNodeOffsetsPerLabel,
        vector<vector<uint64_t>>& deletedNodeOffsetsPerLabel, shared_ptr<spdlog::logger>& logger);

private:
    shared_ptr<spdlog::logger> logger;
    vector<unique_ptr<NodeMetadata>> nodeMetadataPerLabel;
};

} // namespace storage
} // namespace graphflow

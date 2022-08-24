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
    NodeOffsetsInfo(node_offset_t maxNodeOffset_, const vector<node_offset_t>& deletedNodeOffsets_);
    NodeOffsetsInfo(NodeOffsetsInfo& other);

    void setMaxNodeOffset(node_offset_t maxNodeOffset_);

private:
    // This function assumes that it is being called right after ScanNodeID has obtained a morsel
    // and that the nodeID structs in nodeOffsetVector.values have consecutive node offsets and
    // the same tables.
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
    NodeMetadata(table_id_t tableID, node_offset_t maxNodeOffset)
        : NodeMetadata(tableID, maxNodeOffset,
              vector<node_offset_t>() /* no deleted node offsets during initial loading */) {}

    NodeMetadata(table_id_t tableID, node_offset_t maxNodeOffset,
        const vector<node_offset_t>& deletedNodeOffsets);
    // This function assumes that there is a single write transaction. That is why for now we
    // keep the interface simple and no transaction is passed.

    NodeMetadata(const NodeMetadata& other)
        : tableID{other.tableID}, adjListsAndColumns{other.adjListsAndColumns} {
        nodeOffsetsInfo = make_unique<NodeOffsetsInfo>(*other.nodeOffsetsInfo);
    }

    // See the comment for addNode().
    void deleteNode(node_offset_t nodeOffset);

    // This function assumes that it is being called right after ScanNodeID has obtained a morsel
    // and that the nodeID structs in nodeOffsetVector.values have consecutive node offsets and
    // the same tableID.
    inline void setDeletedNodeOffsetsForMorsel(const shared_ptr<ValueVector>& nodeOffsetVector) {
        nodeOffsetsInfo->setDeletedNodeOffsetsForMorsel(nodeOffsetVector);
    }

    inline node_offset_t getMaxNodeOffset() { return nodeOffsetsInfo->maxNodeOffset; }
    inline void setAdjListsAndColumns(
        pair<vector<AdjLists*>, vector<AdjColumn*>> adjListsAndColumns_) {
        adjListsAndColumns = adjListsAndColumns_;
    }

    inline NodeOffsetsInfo* getNodeOffsetInfo() { return nodeOffsetsInfo.get(); }

private:
    void errorIfNodeHasEdges(node_offset_t nodeOffset);

    inline vector<node_offset_t> getDeletedNodeOffsets() {
        return nodeOffsetsInfo->getDeletedNodeOffsets();
    }

private:
    table_id_t tableID;
    // Note: This is initialized explicitly through a call to setAdjListsAndColumns after
    // construction.
    pair<vector<AdjLists*>, vector<AdjColumn*>> adjListsAndColumns;
    unique_ptr<NodeOffsetsInfo> nodeOffsetsInfo;
};

// Manages the disk image of the maxNodeOffsets and deleted node IDs (per node table).
// Note: This class is *not* thread-safe.
class NodesMetadata {

public:
    // Should only be used by database.cpp to start a database from an empty directory.
    NodesMetadata();
    // Should be used when an already loaded database is started from a directory.
    NodesMetadata(const string& directory);

    // Should be used ony by tests;
    explicit NodesMetadata(vector<unique_ptr<NodeMetadata>>& nodeMetadataPerTable_);

    inline NodeMetadata* getNodeMetadata(table_id_t tableID) const {
        return (*nodeMetadataPerTableForReadOnlyTrx)[tableID].get();
    }

    void writeNodesMetadataFileForWALRecord(const string& directory);

    inline bool hasUpdates() { return nodeMetadataPerTableForWriteTrx != nullptr; }

    inline void checkpointInMemoryIfNecessary() {
        lock_t lck{mtx};
        nodeMetadataPerTableForReadOnlyTrx = move(nodeMetadataPerTableForWriteTrx);
    }

    inline void rollbackInMemoryIfNecessary() {
        lock_t lck{mtx};
        nodeMetadataPerTableForWriteTrx.reset();
    }

    static inline void saveInitialNodesMetadataToFile(const string& directory) {
        make_unique<NodesMetadata>()->saveToFile(
            directory, DBFileType::ORIGINAL, TransactionType::READ_ONLY);
    }

    inline void setMaxNodeOffsetForTable(table_id_t tableID, node_offset_t nodeOffset) {
        initNodeMetadataPerTableForWriteTrxIfNecessaryNoLock();
        (*nodeMetadataPerTableForWriteTrx)[tableID]->getNodeOffsetInfo()->setMaxNodeOffset(
            nodeOffset);
    }

    void readFromFile(const string& directory);

    void setAdjListsAndColumns(RelsStore* relsStore);

    void saveToFile(
        const string& directory, DBFileType dbFileType, TransactionType transactionType);

    void initNodeMetadataPerTableForWriteTrxIfNecessaryNoLock();

    // This function assumes that there is a single write transaction. That is why for now we
    // keep the interface simple and no transaction is passed.
    node_offset_t addNode(table_id_t tableID) {
        lock_t lck{mtx};
        initNodeMetadataPerTableForWriteTrxIfNecessaryNoLock();
        return (*nodeMetadataPerTableForWriteTrx)[tableID]->nodeOffsetsInfo->addNode();
    }

    // Refer to the comments for addNode.
    void deleteNode(table_id_t tableID, node_offset_t nodeOffset) {
        lock_t lck{mtx};
        initNodeMetadataPerTableForWriteTrxIfNecessaryNoLock();
        (*nodeMetadataPerTableForWriteTrx)[tableID]->deleteNode(nodeOffset);
    }

    inline node_offset_t getMaxNodeOffset(Transaction* transaction, table_id_t tableID) {
        return getMaxNodeOffset(transaction == nullptr || transaction->isReadOnly() ?
                                    TransactionType::READ_ONLY :
                                    TransactionType::WRITE,
            tableID);
    }

    inline node_offset_t getMaxNodeOffset(TransactionType transactionType, table_id_t tableID) {
        return (transactionType == TransactionType::READ_ONLY ||
                   nodeMetadataPerTableForWriteTrx == nullptr) ?
                   (*nodeMetadataPerTableForReadOnlyTrx)[tableID]->getMaxNodeOffset() :
                   (*nodeMetadataPerTableForWriteTrx)[tableID]->getMaxNodeOffset();
    }

    // This function is only used by storageManager to construct relsStore during start-up, so we
    // can just safely return the maxNodeOffsetPerTable for readOnlyVersion.
    vector<node_offset_t> getMaxNodeOffsetPerTable() const;

    void setDeletedNodeOffsetsForMorsel(Transaction* transaction,
        const shared_ptr<ValueVector>& nodeOffsetVector, table_id_t tableID);

    void addNodeMetadata(NodeTableSchema* tableSchema);

    // This function is only used for testing purpose.
    inline uint32_t getNumNodeMetadataPerTable() const {
        return nodeMetadataPerTableForReadOnlyTrx->size();
    }

private:
    mutex mtx;
    shared_ptr<spdlog::logger> logger;
    unique_ptr<vector<unique_ptr<NodeMetadata>>> nodeMetadataPerTableForReadOnlyTrx;
    unique_ptr<vector<unique_ptr<NodeMetadata>>> nodeMetadataPerTableForWriteTrx;
};

} // namespace storage
} // namespace graphflow

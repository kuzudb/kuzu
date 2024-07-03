#include "graph/on_disk_graph.h"

#include "main/client_context.h"
#include "storage/storage_manager.h"

using namespace kuzu::catalog;
using namespace kuzu::storage;
using namespace kuzu::main;
using namespace kuzu::common;

namespace kuzu {
namespace graph {

static std::unique_ptr<RelTableScanState> getRelScanState(RelDataDirection direction,
    ValueVector* srcVector, ValueVector* dstVector) {
    // Empty columnIDs since we do not scan any rel property.
    auto columnIDs = std::vector<column_id_t>{};
    auto scanState = std::make_unique<RelTableScanState>(columnIDs, direction);
    scanState->nodeIDVector = srcVector;
    scanState->outputVectors.push_back(dstVector);
    return scanState;
}

OnDiskGraphScanState::OnDiskGraphScanState(MemoryManager* mm) {
    srcNodeIDVectorState = DataChunkState::getSingleValueDataChunkState();
    dstNodeIDVectorState = std::make_shared<DataChunkState>();
    srcNodeIDVector = std::make_unique<ValueVector>(LogicalType::INTERNAL_ID(), mm);
    srcNodeIDVector->state = srcNodeIDVectorState;
    dstNodeIDVector = std::make_unique<ValueVector>(LogicalType::INTERNAL_ID(), mm);
    dstNodeIDVector->state = dstNodeIDVectorState;
    fwdScanState =
        getRelScanState(RelDataDirection::FWD, srcNodeIDVector.get(), dstNodeIDVector.get());
    bwdScanState =
        getRelScanState(RelDataDirection::BWD, srcNodeIDVector.get(), dstNodeIDVector.get());
}

OnDiskGraph::OnDiskGraph(ClientContext* context, const GraphEntry& entry)
    : context{context}, graphEntry{entry.copy()}, scanState{context->getMemoryManager()} {
    auto storage = context->getStorageManager();
    auto catalog = context->getCatalog();
    auto transaction = context->getTx();
    for (auto& nodeTableID : graphEntry.nodeTableIDs) {
        nodeIDToNodeTable.insert(
            {nodeTableID, storage->getTable(nodeTableID)->ptrCast<NodeTable>()});
        common::table_id_map_t<storage::RelTable*> fwdRelTables;
        for (auto& relTableID : catalog->getFwdRelTableIDs(transaction, nodeTableID)) {
            if (!graphEntry.containsRelTableID(relTableID)) {
                continue;
            }
            fwdRelTables.insert({relTableID, storage->getTable(relTableID)->ptrCast<RelTable>()});
        }
        nodeTableIDToFwdRelTables.insert({nodeTableID, std::move(fwdRelTables)});
        common::table_id_map_t<storage::RelTable*> bwdRelTables;
        for (auto& relTableID : catalog->getBwdRelTableIDs(transaction, nodeTableID)) {
            if (!graphEntry.containsRelTableID(relTableID)) {
                continue;
            }
            bwdRelTables.insert({relTableID, storage->getTable(relTableID)->ptrCast<RelTable>()});
        }
        nodeTableIDToBwdRelTables.insert({nodeTableID, std::move(bwdRelTables)});
    }
}

OnDiskGraph::OnDiskGraph(const OnDiskGraph& other)
    : context{other.context}, graphEntry{other.graphEntry.copy()},
      nodeIDToNodeTable{other.nodeIDToNodeTable},
      nodeTableIDToFwdRelTables{other.nodeTableIDToFwdRelTables},
      nodeTableIDToBwdRelTables{other.nodeTableIDToBwdRelTables},
      scanState{context->getMemoryManager()} {}

std::vector<table_id_t> OnDiskGraph::getNodeTableIDs() {
    return graphEntry.nodeTableIDs;
}

std::vector<table_id_t> OnDiskGraph::getRelTableIDs() {
    return graphEntry.relTableIDs;
}

offset_t OnDiskGraph::getNumNodes() {
    offset_t numNodes = 0u;
    for (auto id : getNodeTableIDs()) {
        numNodes += getNumNodes(id);
    }
    return numNodes;
}

offset_t OnDiskGraph::getNumNodes(table_id_t id) {
    KU_ASSERT(nodeIDToNodeTable.contains(id));
    return nodeIDToNodeTable.at(id)->getNumTuples(context->getTx());
}

std::vector<RelTableIDInfo> OnDiskGraph::getRelTableIDInfos() {
    std::vector<RelTableIDInfo> result;
    for (auto& [fromNodeTableID, relTables] : nodeTableIDToFwdRelTables) {
        for (auto& [relTableID, _] : relTables) {
            result.push_back(
                {fromNodeTableID, relTableID, relTables.at(relTableID)->getToNodeTableID()});
        }
    }
    return result;
}

std::vector<nodeID_t> OnDiskGraph::scanFwd(nodeID_t nodeID) {
    KU_ASSERT(nodeTableIDToFwdRelTables.contains(nodeID.tableID));
    auto& relTables = nodeTableIDToFwdRelTables.at(nodeID.tableID);
    std::vector<common::nodeID_t> result;
    for (auto& [_, relTable] : relTables) {
        scan(nodeID, relTable, *scanState.fwdScanState, result);
    }
    return result;
}

std::vector<nodeID_t> OnDiskGraph::scanFwd(nodeID_t nodeID, table_id_t relTableID) {
    KU_ASSERT(nodeTableIDToFwdRelTables.contains(nodeID.tableID));
    auto& relTables = nodeTableIDToFwdRelTables.at(nodeID.tableID);
    KU_ASSERT(relTables.contains(relTableID));
    auto relTable = relTables.at(relTableID);
    std::vector<common::nodeID_t> result;
    scan(nodeID, relTable, *scanState.fwdScanState, result);
    return result;
}

std::vector<nodeID_t> OnDiskGraph::scanBwd(nodeID_t nodeID) {
    KU_ASSERT(nodeTableIDToBwdRelTables.contains(nodeID.tableID));
    auto& relTables = nodeTableIDToBwdRelTables.at(nodeID.tableID);
    std::vector<common::nodeID_t> result;
    for (auto& [_, relTable] : relTables) {
        scan(nodeID, relTable, *scanState.bwdScanState, result);
    }
    return result;
}

std::vector<nodeID_t> OnDiskGraph::scanBwd(nodeID_t nodeID, table_id_t relTableID) {
    KU_ASSERT(nodeTableIDToBwdRelTables.contains(nodeID.tableID));
    auto& relTables = nodeTableIDToBwdRelTables.at(nodeID.tableID);
    auto relTable = relTables.at(relTableID);
    std::vector<common::nodeID_t> result;
    scan(nodeID, relTable, *scanState.bwdScanState, result);
    return result;
}

void OnDiskGraph::scan(nodeID_t nodeID, storage::RelTable* relTable,
    RelTableScanState& relTableScanState, std::vector<nodeID_t>& nbrNodeIDs) {
    scanState.srcNodeIDVector->setValue<nodeID_t>(0, nodeID);
    relTableScanState.dataScanState->resetState();
    relTable->initializeScanState(context->getTx(), relTableScanState);
    auto& dstSelVector = scanState.dstNodeIDVector->state->getSelVector();
    while (relTableScanState.hasMoreToRead(context->getTx())) {
        relTable->scan(context->getTx(), relTableScanState);
        KU_ASSERT(dstSelVector.isUnfiltered());
        for (auto i = 0u; i < dstSelVector.getSelSize(); ++i) {
            auto nbrID = scanState.dstNodeIDVector->getValue<nodeID_t>(i);
            nbrNodeIDs.push_back(nbrID);
        }
    }
}

} // namespace graph
} // namespace kuzu

#include "graph/on_disk_graph.h"

#include <memory>

#include "common/assert.h"
#include "common/vector/value_vector.h"
#include "graph/graph.h"
#include "main/client_context.h"
#include "storage/storage_manager.h"
#include "storage/store/rel_table.h"

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
    auto columns = std::vector<Column*>{};
    auto scanState = std::make_unique<RelTableScanState>(INVALID_TABLE_ID, columnIDs, columns,
        nullptr, nullptr, direction);
    scanState->nodeIDVector = srcVector;
    scanState->outputVectors.push_back(dstVector);
    return scanState;
}

OnDiskGraphScanState::OnDiskGraphScanState(ValueVector* srcNodeIDVector,
    ValueVector* dstNodeIDVector) {
    fwdScanState = getRelScanState(RelDataDirection::FWD, srcNodeIDVector, dstNodeIDVector);
    bwdScanState = getRelScanState(RelDataDirection::BWD, srcNodeIDVector, dstNodeIDVector);
}

OnDiskGraphScanStates::OnDiskGraphScanStates(std::span<table_id_t> tableIDs, MemoryManager* mm) {
    scanStates.reserve(tableIDs.size());
    srcNodeIDVectorState = DataChunkState::getSingleValueDataChunkState();
    dstNodeIDVectorState = std::make_shared<DataChunkState>();
    srcNodeIDVector = std::make_unique<ValueVector>(LogicalType::INTERNAL_ID(), mm);
    srcNodeIDVector->state = srcNodeIDVectorState;
    dstNodeIDVector = std::make_unique<ValueVector>(LogicalType::INTERNAL_ID(), mm);
    dstNodeIDVector->state = dstNodeIDVectorState;

    for (auto tableID : tableIDs) {
        scanStates.emplace_back(std::make_pair(tableID,
            OnDiskGraphScanState(srcNodeIDVector.get(), dstNodeIDVector.get())));
    }
}

OnDiskGraph::OnDiskGraph(ClientContext* context, const GraphEntry& entry)
    : context{context}, graphEntry{entry.copy()} {
    auto storage = context->getStorageManager();
    auto catalog = context->getCatalog();
    auto transaction = context->getTx();
    for (auto& nodeTableID : graphEntry.nodeTableIDs) {
        nodeIDToNodeTable.insert(
            {nodeTableID, storage->getTable(nodeTableID)->ptrCast<NodeTable>()});
        table_id_map_t<RelTable*> fwdRelTables;
        for (auto& relTableID : catalog->getFwdRelTableIDs(transaction, nodeTableID)) {
            if (!graphEntry.containsRelTableID(relTableID)) {
                continue;
            }
            fwdRelTables.insert({relTableID, storage->getTable(relTableID)->ptrCast<RelTable>()});
        }
        nodeTableIDToFwdRelTables.insert({nodeTableID, std::move(fwdRelTables)});
        table_id_map_t<RelTable*> bwdRelTables;
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
      nodeTableIDToBwdRelTables{other.nodeTableIDToBwdRelTables} {}

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
    return nodeIDToNodeTable.at(id)->getNumRows();
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

std::unique_ptr<GraphScanState> OnDiskGraph::prepareScan(table_id_t relTableID) {
    return std::unique_ptr<OnDiskGraphScanStates>(
        new OnDiskGraphScanStates(std::span(&relTableID, 1), context->getMemoryManager()));
}

std::unique_ptr<GraphScanState> OnDiskGraph::prepareMultiTableScanFwd(
    std::span<table_id_t> nodeTableIDs) {
    std::unordered_set<table_id_t> relTableIDSet;
    std::vector<table_id_t> relTableIDs;
    for (auto tableID : nodeTableIDs) {
        auto& relTables = nodeTableIDToFwdRelTables.at(tableID);
        for (auto& [tableID, _] : relTables) {
            if (!relTableIDSet.contains(tableID)) {
                relTableIDSet.insert(tableID);
                relTableIDs.push_back(tableID);
            }
        }
    }
    return std::unique_ptr<OnDiskGraphScanStates>(
        new OnDiskGraphScanStates(std::span(relTableIDs), context->getMemoryManager()));
}

std::unique_ptr<GraphScanState> OnDiskGraph::prepareMultiTableScanBwd(
    std::span<table_id_t> nodeTableIDs) {
    std::unordered_set<table_id_t> relTableIDSet;
    std::vector<table_id_t> relTableIDs;
    for (auto tableID : nodeTableIDs) {
        auto& relTables = nodeTableIDToBwdRelTables.at(tableID);
        for (auto& [tableID, _] : relTables) {
            if (!relTableIDSet.contains(tableID)) {
                relTableIDSet.insert(tableID);
                relTableIDs.push_back(tableID);
            }
        }
    }
    return std::unique_ptr<OnDiskGraphScanStates>(
        new OnDiskGraphScanStates(std::span(relTableIDs), context->getMemoryManager()));
}

std::vector<nodeID_t> OnDiskGraph::scanFwd(nodeID_t, GraphScanState&) {
    // auto& onDiskScanState = ku_dynamic_cast<GraphScanState&, OnDiskGraphScanStates&>(state);
    // KU_ASSERT(nodeTableIDToFwdRelTables.contains(nodeID.tableID));
    // auto& relTables = nodeTableIDToFwdRelTables.at(nodeID.tableID);
    std::vector<nodeID_t> result;
    // for (auto& [tableID, scanState] : onDiskScanState.scanStates) {
    //     ku_dynamic_cast<TableDataScanState*, RelDataReadState*>(
    //         scanState.fwdScanState->dataScanState.get())
    //         ->randomAccess = false;
    //     auto relTablePair = relTables.find(tableID);
    //     if (relTablePair != relTables.end()) {
    //         scan(nodeID, relTablePair->second, onDiskScanState, *scanState.fwdScanState, result);
    //     }
    // }
    return result;
}

std::vector<nodeID_t> OnDiskGraph::scanFwdRandom(nodeID_t, GraphScanState&) {
    // auto& onDiskScanState = ku_dynamic_cast<GraphScanState&, OnDiskGraphScanStates&>(state);
    // KU_ASSERT(nodeTableIDToFwdRelTables.contains(nodeID.tableID));
    // auto& relTables = nodeTableIDToFwdRelTables.at(nodeID.tableID);
    std::vector<nodeID_t> result;
    // for (auto& [tableID, scanState] : onDiskScanState.scanStates) {
    //     ku_dynamic_cast<TableDataScanState*, RelDataReadState*>(
    //         scanState.fwdScanState->dataScanState.get())
    //         ->randomAccess = true;
    //     auto relTablePair = relTables.find(tableID);
    //     if (relTablePair != relTables.end()) {
    //         scan(nodeID, relTablePair->second, onDiskScanState, *scanState.fwdScanState, result);
    //     }
    // }
    return result;
}

std::vector<nodeID_t> OnDiskGraph::scanBwd(nodeID_t, GraphScanState&) {
    // auto& onDiskScanState = ku_dynamic_cast<GraphScanState&, OnDiskGraphScanStates&>(state);
    // KU_ASSERT(nodeTableIDToBwdRelTables.contains(nodeID.tableID));
    // auto& relTables = nodeTableIDToBwdRelTables.at(nodeID.tableID);
    std::vector<nodeID_t> result;
    // for (auto& [tableID, scanState] : onDiskScanState.scanStates) {
    //     ku_dynamic_cast<TableDataScanState*, RelDataReadState*>(
    //         scanState.bwdScanState->dataScanState.get())
    //         ->randomAccess = false;
    //     auto relTablePair = relTables.find(tableID);
    //     if (relTablePair != relTables.end()) {
    //         scan(nodeID, relTablePair->second, onDiskScanState, *scanState.bwdScanState, result);
    //     }
    // }
    return result;
}

std::vector<nodeID_t> OnDiskGraph::scanBwdRandom(nodeID_t, GraphScanState&) {
    // auto& onDiskScanState = ku_dynamic_cast<GraphScanState&, OnDiskGraphScanStates&>(state);
    // KU_ASSERT(nodeTableIDToBwdRelTables.contains(nodeID.tableID));
    // auto& relTables = nodeTableIDToBwdRelTables.at(nodeID.tableID);
    std::vector<nodeID_t> result;
    // for (auto& [tableID, scanState] : onDiskScanState.scanStates) {
    //     ku_dynamic_cast<TableDataScanState*, RelDataReadState*>(
    //         scanState.bwdScanState->dataScanState.get())
    //         ->randomAccess = true;
    //     auto relTablePair = relTables.find(tableID);
    //     if (relTablePair != relTables.end()) {
    //         scan(nodeID, relTablePair->second, onDiskScanState, *scanState.bwdScanState, result);
    //     }
    // }
    return result;
}

void OnDiskGraph::scan(nodeID_t, RelTable*, OnDiskGraphScanStates&, RelTableScanState&,
    std::vector<nodeID_t>&) {
    // scanState.srcNodeIDVector->setValue<nodeID_t>(0, nodeID);
    // relTableScanState.resetState();
    // relTable->initializeScanState(context->getTx(), relTableScanState);
    // while (relTableScanState.source != TableScanSource::NONE &&
    //        relTable->scan(context->getTx(), relTableScanState)) {
    //     if (relTableScanState.outState->getSelVector().getSelSize() > 0) {
    //         for (auto i = 0u; i < relTableScanState.outState->getSelVector().getSelSize(); ++i) {
    //             auto nbrID = relTableScanState.IDVector->getValue<nodeID_t>(i);
    //             nbrNodeIDs.push_back(nbrID);
    //         }
    //     }
    // }
}

} // namespace graph
} // namespace kuzu

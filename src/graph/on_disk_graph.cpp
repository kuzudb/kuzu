#include "graph/on_disk_graph.h"

#include <cstdint>
#include <memory>

#include "common/assert.h"
#include "common/enums/rel_direction.h"
#include "common/types/types.h"
#include "common/vector/value_vector.h"
#include "graph/graph.h"
#include "main/client_context.h"
#include "storage/storage_manager.h"
#include "storage/store/rel_table.h"
#include "storage/store/table.h"

using namespace kuzu::catalog;
using namespace kuzu::storage;
using namespace kuzu::main;
using namespace kuzu::common;

namespace kuzu {
namespace graph {

static std::unique_ptr<RelTableScanState> getRelScanState(MemoryManager& mm, const RelTable& table,
    RelDataDirection direction, ValueVector* srcVector, ValueVector* dstVector,
    ValueVector* relIDVector) {
    auto columnIDs = std::vector<column_id_t>{NBR_ID_COLUMN_ID, REL_ID_COLUMN_ID};
    auto columns = std::vector<Column*>{};
    for (const auto columnID : columnIDs) {
        columns.push_back(table.getColumn(columnID, direction));
    }
    auto scanState = std::make_unique<RelTableScanState>(mm, table.getTableID(), columnIDs, columns,
        table.getCSROffsetColumn(direction), table.getCSRLengthColumn(direction), direction);
    scanState->nodeIDVector = srcVector;
    scanState->outputVectors.push_back(dstVector);
    scanState->outputVectors.push_back(relIDVector);
    scanState->outState = dstVector->state.get();
    return scanState;
}

OnDiskGraphScanState::OnDiskGraphScanState(ClientContext* context, RelTable& table,
    ValueVector* srcNodeIDVector, ValueVector* dstNodeIDVector, ValueVector* relIDVector)
    : fwdIterator{context, &table,
          getRelScanState(*context->getMemoryManager(), table, RelDataDirection::FWD,
              srcNodeIDVector, dstNodeIDVector, relIDVector)},
      bwdIterator{context, &table,
          getRelScanState(*context->getMemoryManager(), table, RelDataDirection::BWD,
              srcNodeIDVector, dstNodeIDVector, relIDVector)} {}

OnDiskGraphScanStates::OnDiskGraphScanStates(ClientContext* context, std::span<RelTable*> tables)
    : iteratorIndex{0}, direction{RelDataDirection::FWD} {
    scanStates.reserve(tables.size());
    srcNodeIDVectorState = DataChunkState::getSingleValueDataChunkState();
    dstNodeIDVectorState = std::make_shared<DataChunkState>();
    srcNodeIDVector =
        std::make_unique<ValueVector>(LogicalType::INTERNAL_ID(), context->getMemoryManager());
    srcNodeIDVector->state = srcNodeIDVectorState;
    dstNodeIDVector =
        std::make_unique<ValueVector>(LogicalType::INTERNAL_ID(), context->getMemoryManager());
    dstNodeIDVector->state = dstNodeIDVectorState;
    relIDVector =
        std::make_unique<ValueVector>(LogicalType::INTERNAL_ID(), context->getMemoryManager());
    relIDVector->state = dstNodeIDVectorState;

    for (auto table : tables) {
        scanStates.emplace_back(std::make_pair(table->getTableID(),
            OnDiskGraphScanState(context, *table, srcNodeIDVector.get(), dstNodeIDVector.get(),
                relIDVector.get())));
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

std::vector<table_id_t> OnDiskGraph::getNodeTableIDs() {
    return graphEntry.nodeTableIDs;
}

std::vector<table_id_t> OnDiskGraph::getRelTableIDs() {
    return graphEntry.relTableIDs;
}

std::unordered_map<common::table_id_t, uint64_t> OnDiskGraph::getNodeTableIDAndNumNodes() {
    std::unordered_map<common::table_id_t, uint64_t> retVal;
    for (common::table_id_t tableID : getNodeTableIDs()) {
        retVal[tableID] = getNumNodes(tableID);
    }
    return retVal;
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
    auto relTable = context->getStorageManager()->getTable(relTableID)->ptrCast<RelTable>();
    return std::unique_ptr<OnDiskGraphScanStates>(
        new OnDiskGraphScanStates(context, std::span(&relTable, 1)));
}

std::unique_ptr<GraphScanState> OnDiskGraph::prepareMultiTableScanFwd(
    std::span<table_id_t> nodeTableIDs) {
    std::unordered_set<table_id_t> relTableIDSet;
    std::vector<RelTable*> tables;
    for (auto tableID : nodeTableIDs) {
        auto& relTables = nodeTableIDToFwdRelTables.at(tableID);
        for (auto& [tableID, table] : relTables) {
            if (!relTableIDSet.contains(tableID)) {
                relTableIDSet.insert(tableID);
                tables.push_back(table);
            }
        }
    }
    return std::unique_ptr<OnDiskGraphScanStates>(
        new OnDiskGraphScanStates(context, std::span(tables)));
}

std::unique_ptr<GraphScanState> OnDiskGraph::prepareMultiTableScanBwd(
    std::span<table_id_t> nodeTableIDs) {
    std::unordered_set<table_id_t> relTableIDSet;
    std::vector<RelTable*> tables;
    for (auto tableID : nodeTableIDs) {
        auto& relTables = nodeTableIDToBwdRelTables.at(tableID);
        for (auto& [tableID, table] : relTables) {
            if (!relTableIDSet.contains(tableID)) {
                relTableIDSet.insert(tableID);
                tables.push_back(table);
            }
        }
    }
    return std::unique_ptr<OnDiskGraphScanStates>(
        new OnDiskGraphScanStates(context, std::span(tables)));
}

Graph::Iterator OnDiskGraph::scanFwd(nodeID_t nodeID, GraphScanState& state) {
    auto& onDiskScanState = ku_dynamic_cast<OnDiskGraphScanStates&>(state);
    onDiskScanState.srcNodeIDVector->setValue<nodeID_t>(0, nodeID);
    onDiskScanState.dstNodeIDVectorState->getSelVectorUnsafe().setSelSize(0);
    KU_ASSERT(nodeTableIDToFwdRelTables.contains(nodeID.tableID));
    auto& relTables = nodeTableIDToFwdRelTables.at(nodeID.tableID);
    for (auto& [tableID, scanState] : onDiskScanState.scanStates) {
        auto relTablePair = relTables.find(tableID);
        if (relTablePair != relTables.end()) {
            scanState.fwdIterator.initScan();
        }
    }
    onDiskScanState.startScan(common::RelDataDirection::FWD);
    return Graph::Iterator(&onDiskScanState);
}

std::vector<nodeID_t> OnDiskGraph::scanFwdRandom(nodeID_t, GraphScanState&) {
    // auto& onDiskScanState = ku_dynamic_cast<OnDiskGraphScanStates&>(state);
    // KU_ASSERT(nodeTableIDToFwdRelTables.contains(nodeID.tableID));
    // auto& relTables = nodeTableIDToFwdRelTables.at(nodeID.tableID);
    std::vector<nodeID_t> result;
    // for (auto& [tableID, scanState] : onDiskScanState.scanStates) {
    //     ku_dynamic_cast<RelDataReadState*>(
    //         scanState.fwdScanState->dataScanState.get())
    //         ->randomAccess = true;
    //     auto relTablePair = relTables.find(tableID);
    //     if (relTablePair != relTables.end()) {
    //         scan(nodeID, relTablePair->second, onDiskScanState, *scanState.fwdScanState, result);
    //     }
    // }
    return result;
}

Graph::Iterator OnDiskGraph::scanBwd(nodeID_t nodeID, GraphScanState& state) {
    auto& onDiskScanState = ku_dynamic_cast<OnDiskGraphScanStates&>(state);
    onDiskScanState.srcNodeIDVector->setValue<nodeID_t>(0, nodeID);
    onDiskScanState.dstNodeIDVectorState->getSelVectorUnsafe().setSelSize(0);
    KU_ASSERT(nodeTableIDToBwdRelTables.contains(nodeID.tableID));
    auto& relTables = nodeTableIDToBwdRelTables.at(nodeID.tableID);
    for (auto& [tableID, scanState] : onDiskScanState.scanStates) {
        auto relTablePair = relTables.find(tableID);
        if (relTablePair != relTables.end()) {
            scanState.bwdIterator.initScan();
        }
    }
    onDiskScanState.startScan(common::RelDataDirection::BWD);
    return Graph::Iterator(&onDiskScanState);
}

std::vector<nodeID_t> OnDiskGraph::scanBwdRandom(nodeID_t, GraphScanState&) {
    // auto& onDiskScanState = ku_dynamic_cast<OnDiskGraphScanStates&>(state);
    // KU_ASSERT(nodeTableIDToBwdRelTables.contains(nodeID.tableID));
    // auto& relTables = nodeTableIDToBwdRelTables.at(nodeID.tableID);
    std::vector<nodeID_t> result;
    // for (auto& [tableID, scanState] : onDiskScanState.scanStates) {
    //     ku_dynamic_cast<RelDataReadState*>(
    //         scanState.bwdScanState->dataScanState.get())
    //         ->randomAccess = true;
    //     auto relTablePair = relTables.find(tableID);
    //     if (relTablePair != relTables.end()) {
    //         scan(nodeID, relTablePair->second, onDiskScanState, *scanState.bwdScanState, result);
    //     }
    // }
    return result;
}

bool OnDiskGraphScanState::InnerIterator::next() {
    return tableScanState->source != TableScanSource::NONE &&
           relTable->scan(context->getTx(), *tableScanState) && getSelVector().getSelSize() > 0;
}

OnDiskGraphScanState::InnerIterator::InnerIterator(const main::ClientContext* context,
    storage::RelTable* relTable, std::unique_ptr<storage::RelTableScanState> tableScanState)
    : context{context}, relTable{relTable}, tableScanState{std::move(tableScanState)} {}

void OnDiskGraphScanState::InnerIterator::initScan() {
    relTable->initScanState(context->getTx(), *tableScanState);
}

bool OnDiskGraphScanStates::next() {
    while (iteratorIndex < scanStates.size()) {
        if (getInnerIterator().next()) {
            return true;
        }
        iteratorIndex++;
    }
    return false;
};

} // namespace graph
} // namespace kuzu

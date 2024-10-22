#include "graph/on_disk_graph.h"

#include <memory>

#include "binder/expression/property_expression.h"
#include "common/assert.h"
#include "common/enums/rel_direction.h"
#include "common/types/types.h"
#include "common/vector/value_vector.h"
#include "expression_evaluator/expression_evaluator.h"
#include "graph/graph.h"
#include "main/client_context.h"
#include "planner/operator/schema.h"
#include "processor/expression_mapper.h"
#include "storage/storage_manager.h"
#include "storage/store/rel_table.h"
#include "storage/store/table.h"

using namespace kuzu::catalog;
using namespace kuzu::storage;
using namespace kuzu::main;
using namespace kuzu::common;
using namespace kuzu::planner;
using namespace kuzu::processor;
using namespace kuzu::binder;

namespace kuzu {
namespace graph {

static std::unique_ptr<RelTableScanState> getRelScanState(MemoryManager& mm,
    const TableCatalogEntry& relEntry, const RelTable& table, RelDataDirection direction,
    ValueVector* srcVector, ValueVector* dstVector, ValueVector* relIDVector,
    expression_vector properties, const Schema& schema, const ResultSet& resultSet) {
    auto columnIDs = std::vector<column_id_t>{NBR_ID_COLUMN_ID, REL_ID_COLUMN_ID};
    for (auto property : properties) {
        columnIDs.push_back(property->constCast<PropertyExpression>().getColumnID(relEntry));
    }
    auto columns = std::vector<const Column*>{};
    for (const auto columnID : columnIDs) {
        columns.push_back(table.getColumn(columnID, direction));
    }
    auto scanState = std::make_unique<RelTableScanState>(mm, table.getTableID(), columnIDs, columns,
        table.getCSROffsetColumn(direction), table.getCSRLengthColumn(direction), direction);
    scanState->nodeIDVector = srcVector;
    scanState->outputVectors.push_back(dstVector);
    scanState->outputVectors.push_back(relIDVector);
    for (auto& property : properties) {
        auto pos = DataPos(schema.getExpressionPos(*property));
        auto vector = resultSet.getValueVector(pos).get();
        scanState->outputVectors.push_back(vector);
    }
    scanState->outState = dstVector->state.get();
    return scanState;
}

OnDiskGraphScanStates::OnDiskGraphScanStates(ClientContext* context, std::span<RelTable*> tables,
    const GraphEntry& graphEntry)
    : iteratorIndex{0}, direction{RelDataDirection::INVALID} {
    auto schema = graphEntry.getRelPropertiesSchema();
    auto descriptor = ResultSetDescriptor(&schema);
    auto resultSet = ResultSet(&descriptor, context->getMemoryManager());
    KU_ASSERT(resultSet.dataChunks.size() == 1);
    auto state = resultSet.getDataChunk(0)->state;
    srcNodeIDVector =
        std::make_unique<ValueVector>(LogicalType::INTERNAL_ID(), context->getMemoryManager());
    srcNodeIDVector->state = DataChunkState::getSingleValueDataChunkState();
    dstNodeIDVector =
        std::make_unique<ValueVector>(LogicalType::INTERNAL_ID(), context->getMemoryManager());
    dstNodeIDVector->state = state;
    relIDVector =
        std::make_unique<ValueVector>(LogicalType::INTERNAL_ID(), context->getMemoryManager());
    relIDVector->state = state;
    if (graphEntry.hasRelPredicate()) {
        auto mapper = ExpressionMapper(&schema);
        relPredicateEvaluator = mapper.getEvaluator(graphEntry.getRelPredicate());
        relPredicateEvaluator->init(resultSet, context);
    }
    scanStates.reserve(tables.size());
    for (auto table : tables) {
        auto relEntry = graphEntry.getRelEntry(table->getTableID());
        auto fwdState = getRelScanState(*context->getMemoryManager(), *relEntry, *table,
            RelDataDirection::FWD, srcNodeIDVector.get(), dstNodeIDVector.get(), relIDVector.get(),
            graphEntry.getRelProperties(), schema, resultSet);
        auto bwdState = getRelScanState(*context->getMemoryManager(), *relEntry, *table,
            RelDataDirection::BWD, srcNodeIDVector.get(), dstNodeIDVector.get(), relIDVector.get(),
            graphEntry.getRelProperties(), schema, resultSet);
        scanStates.emplace_back(table->getTableID(),
            OnDiskGraphScanState{context, *table, std::move(fwdState), std::move(bwdState)});
    }
}

OnDiskGraph::OnDiskGraph(ClientContext* context, const GraphEntry& entry)
    : context{context}, graphEntry{entry.copy()} {
    auto storage = context->getStorageManager();
    auto catalog = context->getCatalog();
    auto transaction = context->getTx();
    for (auto& nodeEntry : graphEntry.nodeEntries) {
        auto nodeTableID = nodeEntry->getTableID();
        nodeIDToNodeTable.insert(
            {nodeTableID, storage->getTable(nodeTableID)->ptrCast<NodeTable>()});
        table_id_map_t<RelTable*> fwdRelTables;
        for (auto& relTableID : catalog->getFwdRelTableIDs(transaction, nodeTableID)) {
            if (!graphEntry.hasRelEntry(relTableID)) {
                continue;
            }
            fwdRelTables.insert({relTableID, storage->getTable(relTableID)->ptrCast<RelTable>()});
        }
        nodeTableIDToFwdRelTables.insert({nodeTableID, std::move(fwdRelTables)});
        table_id_map_t<RelTable*> bwdRelTables;
        for (auto& relTableID : catalog->getBwdRelTableIDs(transaction, nodeTableID)) {
            if (!graphEntry.hasRelEntry(relTableID)) {
                continue;
            }
            bwdRelTables.insert({relTableID, storage->getTable(relTableID)->ptrCast<RelTable>()});
        }
        nodeTableIDToBwdRelTables.insert({nodeTableID, std::move(bwdRelTables)});
    }
}

std::vector<table_id_t> OnDiskGraph::getNodeTableIDs() {
    std::vector<table_id_t> result;
    for (auto& entry : graphEntry.nodeEntries) {
        result.push_back(entry->getTableID());
    }
    return result;
}

std::vector<table_id_t> OnDiskGraph::getRelTableIDs() {
    std::vector<table_id_t> result;
    for (auto& entry : graphEntry.relEntries) {
        result.push_back(entry->getTableID());
    }
    return result;
}

std::unordered_map<common::table_id_t, offset_t> OnDiskGraph::getNodeTableIDAndNumNodes() {
    std::unordered_map<common::table_id_t, offset_t> retVal;
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
        new OnDiskGraphScanStates(context, std::span(&relTable, 1), graphEntry));
}

std::unique_ptr<GraphScanState> OnDiskGraph::prepareMultiTableScanFwd(
    std::span<table_id_t> nodeTableIDs) {
    std::unordered_set<table_id_t> relTableIDSet;
    std::vector<RelTable*> tables;
    for (auto nodeTableID : nodeTableIDs) {
        for (auto& [tableID, table] : nodeTableIDToFwdRelTables.at(nodeTableID)) {
            if (!relTableIDSet.contains(tableID)) {
                relTableIDSet.insert(tableID);
                tables.push_back(table);
            }
        }
    }
    return std::unique_ptr<OnDiskGraphScanStates>(
        new OnDiskGraphScanStates(context, std::span(tables), graphEntry));
}

std::unique_ptr<GraphScanState> OnDiskGraph::prepareMultiTableScanBwd(
    std::span<table_id_t> nodeTableIDs) {
    std::unordered_set<table_id_t> relTableIDSet;
    std::vector<RelTable*> tables;
    for (auto nodeTableID : nodeTableIDs) {
        for (auto& [tableID, table] : nodeTableIDToBwdRelTables.at(nodeTableID)) {
            if (!relTableIDSet.contains(tableID)) {
                relTableIDSet.insert(tableID);
                tables.push_back(table);
            }
        }
    }
    return std::unique_ptr<OnDiskGraphScanStates>(
        new OnDiskGraphScanStates(context, std::span(tables), graphEntry));
}

Graph::Iterator OnDiskGraph::scanFwd(nodeID_t nodeID, GraphScanState& state) {
    auto& onDiskScanState = ku_dynamic_cast<OnDiskGraphScanStates&>(state);
    onDiskScanState.srcNodeIDVector->setValue<nodeID_t>(0, nodeID);
    onDiskScanState.dstNodeIDVector->state->getSelVectorUnsafe().setSelSize(0);
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

Graph::Iterator OnDiskGraph::scanBwd(nodeID_t nodeID, GraphScanState& state) {
    auto& onDiskScanState = ku_dynamic_cast<OnDiskGraphScanStates&>(state);
    onDiskScanState.srcNodeIDVector->setValue<nodeID_t>(0, nodeID);
    onDiskScanState.dstNodeIDVector->state->getSelVectorUnsafe().setSelSize(0);
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

bool OnDiskGraphScanState::InnerIterator::next(evaluator::ExpressionEvaluator* predicate) {
    while (true) {
        if (tableScanState->source == TableScanSource::NONE) {
            return false;
        }
        if (!relTable->scan(context->getTx(), *tableScanState)) {
            continue;
        }
        if (predicate != nullptr) {
            predicate->select(tableScanState->outState->getSelVectorUnsafe());
            tableScanState->outState->getSelVectorUnsafe().setToFiltered();
        }
        if (getSelVector().getSelSize() > 0) {
            return true;
        }
    }
}

OnDiskGraphScanState::InnerIterator::InnerIterator(const main::ClientContext* context,
    storage::RelTable* relTable, std::unique_ptr<storage::RelTableScanState> tableScanState)
    : context{context}, relTable{relTable}, tableScanState{std::move(tableScanState)} {}

void OnDiskGraphScanState::InnerIterator::initScan() {
    relTable->initScanState(context->getTx(), *tableScanState);
}

bool OnDiskGraphScanStates::next() {
    while (iteratorIndex < scanStates.size()) {
        if (getInnerIterator().next(relPredicateEvaluator.get())) {
            return true;
        }
        iteratorIndex++;
    }
    return false;
};

} // namespace graph
} // namespace kuzu

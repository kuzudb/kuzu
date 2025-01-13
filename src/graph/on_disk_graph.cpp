#include "graph/on_disk_graph.h"

#include <memory>

#include "binder/expression/property_expression.h"
#include "common/assert.h"
#include "common/cast.h"
#include "common/constants.h"
#include "common/data_chunk/data_chunk_state.h"
#include "common/enums/rel_direction.h"
#include "common/types/types.h"
#include "common/vector/value_vector.h"
#include "expression_evaluator/expression_evaluator.h"
#include "graph/graph.h"
#include "main/client_context.h"
#include "planner/operator/schema.h"
#include "processor/expression_mapper.h"
#include "storage/buffer_manager/memory_manager.h"
#include "storage/local_storage/local_rel_table.h"
#include "storage/local_storage/local_storage.h"
#include "storage/storage_manager.h"
#include "storage/storage_utils.h"
#include "storage/store/column.h"
#include "storage/store/node_table.h"
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

static std::unique_ptr<RelTableScanState> getRelScanState(
    const transaction::Transaction* transaction, MemoryManager& mm,
    const TableCatalogEntry& relEntry, const RelTable& table, RelDataDirection direction,
    ValueVector* srcVector, ValueVector* dstVector, ValueVector* relIDVector,
    expression_vector predicateEdgeProperties, std::optional<column_id_t> edgePropertyID,
    ValueVector* propertyVector, const Schema& schema, const ResultSet& resultSet) {
    auto columnIDs = std::vector<column_id_t>{NBR_ID_COLUMN_ID, REL_ID_COLUMN_ID};
    for (auto property : predicateEdgeProperties) {
        columnIDs.push_back(property->constCast<PropertyExpression>().getColumnID(relEntry));
    }
    if (edgePropertyID) {
        columnIDs.push_back(*edgePropertyID);
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
    for (auto& property : predicateEdgeProperties) {
        auto pos = DataPos(schema.getExpressionPos(*property));
        auto vector = resultSet.getValueVector(pos).get();
        scanState->outputVectors.push_back(vector);
    }
    if (edgePropertyID) {
        scanState->outputVectors.push_back(propertyVector);
    }
    scanState->outState = dstVector->state.get();
    scanState->rowIdxVector->state = dstVector->state;
    if (auto localTable = transaction->getLocalStorage()->getLocalTable(table.getTableID(),
            LocalStorage::NotExistAction::RETURN_NULL)) {
        auto localTableColumnIDs =
            LocalRelTable::rewriteLocalColumnIDs(direction, scanState->columnIDs);
        scanState->localTableScanState = std::make_unique<LocalRelTableScanState>(*scanState,
            localTableColumnIDs, localTable->ptrCast<LocalRelTable>());
    }
    return scanState;
}

OnDiskGraphNbrScanStates::OnDiskGraphNbrScanStates(main::ClientContext* context,
    TableCatalogEntry* tableEntry, const GraphEntry& graphEntry)
    : OnDiskGraphNbrScanStates{context, tableEntry, graphEntry, ""} {}

OnDiskGraphNbrScanStates::OnDiskGraphNbrScanStates(ClientContext* context,
    catalog::TableCatalogEntry* tableEntry, const GraphEntry& graphEntry,
    const std::string& propertyName)
    : direction{RelDataDirection::INVALID} {
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
    std::optional<column_id_t> edgePropertyID;
    if (!propertyName.empty()) {
        // TODO(bmwinger): If there are both a predicate and a custom edgePropertyIndex, they will
        // currently be scanned twice. The propertyVector could simply be one of the vectors used
        // for the predicate.
        auto& propertyDef = tableEntry->getProperty(propertyName);
        propertyVector = std::make_unique<ValueVector>(propertyDef.getType().copy(),
            context->getMemoryManager());
        propertyVector->state = state;
        edgePropertyID = tableEntry->getColumnID(propertyName);
    }
    if (graphEntry.hasRelPredicate()) {
        auto mapper = ExpressionMapper(&schema);
        relPredicateEvaluator = mapper.getEvaluator(graphEntry.getRelPredicate());
        relPredicateEvaluator->init(resultSet, context);
    }
    auto table =
        context->getStorageManager()->getTable(tableEntry->getTableID())->ptrCast<RelTable>();
    std::vector<std::unique_ptr<RelTableScanState>> directedScanStates;
    for (auto dataDirection : tableEntry->ptrCast<RelTableCatalogEntry>()->getRelDataDirections()) {
        directedScanStates.emplace_back(getRelScanState(context->getTransaction(),
            *context->getMemoryManager(), *tableEntry, *table, dataDirection, srcNodeIDVector.get(),
            dstNodeIDVector.get(), relIDVector.get(), graphEntry.getRelProperties(), edgePropertyID,
            propertyVector.get(), schema, resultSet));
    }
    relTableID = tableEntry->getTableID();
    directedScanState =
        OnDiskGraphDirectedNbrScanState{context, *table, std::move(directedScanStates)};
}

OnDiskGraph::OnDiskGraph(ClientContext* context, const GraphEntry& entry)
    : context{context}, graphEntry{entry.copy()} {
    auto storage = context->getStorageManager();
    auto catalog = context->getCatalog();
    auto transaction = context->getTransaction();
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

std::vector<table_id_t> OnDiskGraph::getNodeTableIDs() const {
    std::vector<table_id_t> result;
    for (auto& entry : graphEntry.nodeEntries) {
        result.push_back(entry->getTableID());
    }
    return result;
}

std::vector<table_id_t> OnDiskGraph::getRelTableIDs() const {
    std::vector<table_id_t> result;
    for (auto& entry : graphEntry.relEntries) {
        result.push_back(entry->getTableID());
    }
    return result;
}

table_id_map_t<offset_t> OnDiskGraph::getNumNodesMap(transaction::Transaction* transaction) const {
    table_id_map_t<offset_t> retVal;
    for (auto tableID : getNodeTableIDs()) {
        retVal[tableID] = getNumNodes(transaction, tableID);
    }
    return retVal;
}

offset_t OnDiskGraph::getNumNodes(transaction::Transaction* transaction) const {
    offset_t numNodes = 0u;
    for (auto id : getNodeTableIDs()) {
        numNodes += getNumNodes(transaction, id);
    }
    return numNodes;
}

offset_t OnDiskGraph::getNumNodes(transaction::Transaction* transaction, table_id_t id) const {
    KU_ASSERT(nodeIDToNodeTable.contains(id));
    return nodeIDToNodeTable.at(id)->getNumTotalRows(transaction);
}

std::vector<RelFromToEntryInfo> OnDiskGraph::getRelFromToEntryInfos() {
    std::vector<RelFromToEntryInfo> result;
    auto transaction = context->getTransaction();
    auto catalog = context->getCatalog();
    for (auto& [fromNodeTableID, relTables] : nodeTableIDToFwdRelTables) {
        for (auto& [relTableID, relTable] : relTables) {
            auto fromEntry = catalog->getTableCatalogEntry(transaction, fromNodeTableID);
            auto toEntry = catalog->getTableCatalogEntry(transaction, relTable->getToNodeTableID());
            auto relEntry = catalog->getTableCatalogEntry(transaction, relTableID);
            result.emplace_back(fromEntry, toEntry, relEntry);
        }
    }
    return result;
}

std::unique_ptr<NbrScanState> OnDiskGraph::prepareRelScan(catalog::TableCatalogEntry* tableEntry,
    const std::string& property) {
    return std::make_unique<OnDiskGraphNbrScanStates>(context, tableEntry, graphEntry, property);
}

Graph::EdgeIterator OnDiskGraph::scanFwd(nodeID_t nodeID, NbrScanState& state) {
    auto& onDiskScanState = ku_dynamic_cast<OnDiskGraphNbrScanStates&>(state);
    onDiskScanState.srcNodeIDVector->setValue<nodeID_t>(0, nodeID);
    onDiskScanState.dstNodeIDVector->state->getSelVectorUnsafe().setSelSize(0);
    KU_ASSERT(nodeTableIDToFwdRelTables.contains(nodeID.tableID));
    auto& relTables = nodeTableIDToFwdRelTables.at(nodeID.tableID);
    if (relTables.contains(onDiskScanState.relTableID)) {
        onDiskScanState.directedScanState.getIterator(common::RelDataDirection::FWD).initScan();
    }
    onDiskScanState.startScan(common::RelDataDirection::FWD);
    return Graph::EdgeIterator(&onDiskScanState);
}

Graph::EdgeIterator OnDiskGraph::scanBwd(nodeID_t nodeID, NbrScanState& state) {
    auto& onDiskScanState = ku_dynamic_cast<OnDiskGraphNbrScanStates&>(state);
    onDiskScanState.srcNodeIDVector->setValue<nodeID_t>(0, nodeID);
    onDiskScanState.dstNodeIDVector->state->getSelVectorUnsafe().setSelSize(0);
    KU_ASSERT(nodeTableIDToBwdRelTables.contains(nodeID.tableID));
    auto& relTables = nodeTableIDToBwdRelTables.at(nodeID.tableID);
    if (relTables.contains(onDiskScanState.relTableID)) {
        onDiskScanState.directedScanState.getIterator(common::RelDataDirection::BWD).initScan();
    }
    onDiskScanState.startScan(common::RelDataDirection::BWD);
    return Graph::EdgeIterator(&onDiskScanState);
}

Graph::VertexIterator OnDiskGraph::scanVertices(common::offset_t beginOffset,
    common::offset_t endOffsetExclusive, VertexScanState& state) {
    auto& onDiskVertexScanState = ku_dynamic_cast<OnDiskGraphVertexScanState&>(state);
    onDiskVertexScanState.startScan(beginOffset, endOffsetExclusive);
    return Graph::VertexIterator(&state);
}

std::unique_ptr<VertexScanState> OnDiskGraph::prepareVertexScan(
    catalog::TableCatalogEntry* tableEntry, const std::vector<std::string>& propertiesToScan) {
    return std::make_unique<OnDiskGraphVertexScanState>(*context, tableEntry, propertiesToScan);
}

OnDiskGraphDirectedNbrScanState::InnerIterator& OnDiskGraphDirectedNbrScanState::getIterator(
    RelDataDirection direction) {
    auto directionIdx = RelDirectionUtils::relDirectionToKeyIdx(direction);
    KU_ASSERT(directionIdx < directedIterators.size() &&
              directedIterators[directionIdx].getDirection() == direction);
    return directedIterators[directionIdx];
}

bool OnDiskGraphDirectedNbrScanState::InnerIterator::next(
    evaluator::ExpressionEvaluator* predicate) {
    while (true) {
        if (!relTable->scan(context->getTransaction(), *tableScanState)) {
            return false;
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

OnDiskGraphDirectedNbrScanState::InnerIterator::InnerIterator(const main::ClientContext* context,
    storage::RelTable* relTable, std::unique_ptr<storage::RelTableScanState> tableScanState)
    : context{context}, relTable{relTable}, tableScanState{std::move(tableScanState)} {}

void OnDiskGraphDirectedNbrScanState::InnerIterator::initScan() {
    relTable->initScanState(context->getTransaction(), *tableScanState);
}

bool OnDiskGraphNbrScanStates::next() {
    if (getInnerIterator().next(relPredicateEvaluator.get())) {
        return true;
    }
    return false;
}

OnDiskGraphVertexScanState::OnDiskGraphVertexScanState(ClientContext& context,
    catalog::TableCatalogEntry* tableEntry, const std::vector<std::string>& propertyNames)
    : context{context}, nodeTable{ku_dynamic_cast<const NodeTable&>(
                            *context.getStorageManager()->getTable(tableEntry->getTableID()))},
      numNodesScanned{0}, currentOffset{0}, endOffsetExclusive{0} {
    std::vector<column_id_t> propertyColumnIDs;
    propertyColumnIDs.reserve(propertyNames.size());
    std::vector<const Column*> columns;
    std::vector<LogicalType> types;
    for (const auto& property : propertyNames) {
        auto columnID = tableEntry->getColumnID(property);
        propertyColumnIDs.push_back(columnID);
        columns.push_back(&nodeTable.getColumn(columnID));
        types.push_back(columns.back()->getDataType().copy());
    }
    propertyVectors = nodeTable.constructDataChunk(std::move(types));
    nodeIDVector = std::make_unique<ValueVector>(LogicalType::INTERNAL_ID(),
        context.getMemoryManager(), propertyVectors.state);
    tableScanState = std::make_unique<NodeTableScanState>(tableEntry->getTableID(),
        std::move(propertyColumnIDs), std::move(columns), propertyVectors, nodeIDVector.get());
}

void OnDiskGraphVertexScanState::startScan(common::offset_t beginOffset,
    common::offset_t endOffset) {
    numNodesScanned = 0;
    this->currentOffset = beginOffset;
    this->endOffsetExclusive = endOffset;
    nodeTable.initScanState(context.getTransaction(), *tableScanState, nodeTable.getTableID(),
        beginOffset);
}

bool OnDiskGraphVertexScanState::next() {
    if (currentOffset >= endOffsetExclusive) {
        return false;
    }
    if (currentOffset < endOffsetExclusive &&
        StorageUtils::getNodeGroupIdx(currentOffset) != tableScanState->nodeGroupIdx) {
        startScan(currentOffset, endOffsetExclusive);
    }

    auto endOffset = std::min(endOffsetExclusive,
        StorageUtils::getStartOffsetOfNodeGroup(tableScanState->nodeGroupIdx + 1));
    numNodesScanned = std::min(endOffset - currentOffset, DEFAULT_VECTOR_CAPACITY);
    auto result =
        tableScanState->scanNext(context.getTransaction(), currentOffset, numNodesScanned);
    currentOffset += numNodesScanned;
    return result;
}

} // namespace graph
} // namespace kuzu

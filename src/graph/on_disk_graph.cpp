#include "graph/on_disk_graph.h"

#include "binder/expression/property_expression.h"
#include "catalog/catalog_entry/node_table_catalog_entry.h"
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
#include "storage/local_storage/local_rel_table.h"
#include "storage/local_storage/local_storage.h"
#include "storage/storage_manager.h"
#include "storage/storage_utils.h"
#include "storage/store/column.h"
#include "storage/store/node_table.h"
#include "storage/store/rel_table.h"

using namespace kuzu::catalog;
using namespace kuzu::storage;
using namespace kuzu::main;
using namespace kuzu::common;
using namespace kuzu::planner;
using namespace kuzu::processor;
using namespace kuzu::binder;

namespace kuzu {
namespace graph {

static std::vector<column_id_t> getColumnIDs(const expression_vector& propertyExprs,
    const TableCatalogEntry& relEntry, std::optional<column_id_t> propertyColumnID) {
    auto columnIDs = std::vector<column_id_t>{NBR_ID_COLUMN_ID, REL_ID_COLUMN_ID};
    for (auto expr : propertyExprs) {
        columnIDs.push_back(expr->constCast<PropertyExpression>().getColumnID(relEntry));
    }
    if (propertyColumnID) {
        columnIDs.push_back(*propertyColumnID);
    }
    return columnIDs;
}

static std::vector<const Column*> getColumns(const std::vector<column_id_t>& columnIDs,
    const RelTable& table, RelDataDirection direction) {
    auto columns = std::vector<const Column*>{};
    for (const auto columnID : columnIDs) {
        columns.push_back(table.getColumn(columnID, direction));
    }
    return columns;
}

OnDiskGraphNbrScanState::OnDiskGraphNbrScanState(main::ClientContext* context,
    TableCatalogEntry* tableEntry, const GraphEntry& graphEntry)
    : OnDiskGraphNbrScanState{context, tableEntry, graphEntry, ""} {}

OnDiskGraphNbrScanState::OnDiskGraphNbrScanState(ClientContext* context,
    catalog::TableCatalogEntry* tableEntry, const GraphEntry& graphEntry,
    const std::string& propertyName, bool randomLookup) {
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
    for (auto dataDirection : tableEntry->ptrCast<RelTableCatalogEntry>()->getRelDataDirections()) {
        auto columnIDs = getColumnIDs(graphEntry.getRelProperties(), *tableEntry, edgePropertyID);
        auto columns = getColumns(columnIDs, *table, dataDirection);
        auto scanState = std::make_unique<RelTableScanState>(*context->getMemoryManager(),
            table->getTableID(), columnIDs, columns, table->getCSROffsetColumn(dataDirection),
            table->getCSRLengthColumn(dataDirection), dataDirection);
        // Initialize vectors to scanState
        scanState->nodeIDVector = srcNodeIDVector.get();
        scanState->outputVectors.push_back(dstNodeIDVector.get());
        scanState->outputVectors.push_back(relIDVector.get());
        for (auto& property : graphEntry.getRelProperties()) {
            auto pos = DataPos(schema.getExpressionPos(*property));
            scanState->outputVectors.push_back(resultSet.getValueVector(pos).get());
        }
        if (edgePropertyID) {
            scanState->outputVectors.push_back(propertyVector.get());
        }
        scanState->outState = dstNodeIDVector->state.get();
        scanState->rowIdxVector->state = dstNodeIDVector->state;
        scanState->randomLookup = randomLookup;
        // Initialize local transaction to scanState
        if (auto localTable = context->getTransaction()->getLocalStorage()->getLocalTable(
                table->getTableID(), LocalStorage::NotExistAction::RETURN_NULL)) {
            auto localTableColumnIDs =
                LocalRelTable::rewriteLocalColumnIDs(dataDirection, scanState->columnIDs);
            scanState->localTableScanState = std::make_unique<LocalRelTableScanState>(*scanState,
                localTableColumnIDs, localTable->ptrCast<LocalRelTable>());
        }
        directedIterators.emplace_back(context, table, std::move(scanState));
    }
}

OnDiskGraph::OnDiskGraph(ClientContext* context, GraphEntry entry)
    : context{context}, graphEntry{std::move(entry)} {
    auto storage = context->getStorageManager();
    auto catalog = context->getCatalog();
    auto transaction = context->getTransaction();
    for (auto& entry : graphEntry.nodeEntries) {
        auto& nodeEntry = entry->constCast<NodeTableCatalogEntry>();
        auto nodeTableID = nodeEntry.getTableID();
        nodeIDToNodeTable.insert(
            {nodeTableID, storage->getTable(nodeTableID)->ptrCast<NodeTable>()});
        table_id_map_t<RelTable*> fwdRelTables;
        for (auto& relTableID : nodeEntry.getFwdRelTableIDs(catalog, transaction)) {
            if (!graphEntry.hasRelEntry(relTableID)) {
                continue;
            }
            fwdRelTables.insert({relTableID, storage->getTable(relTableID)->ptrCast<RelTable>()});
        }
        nodeTableIDToFwdRelTables.insert({nodeTableID, std::move(fwdRelTables)});
        table_id_map_t<RelTable*> bwdRelTables;
        for (auto& relTableID : nodeEntry.getBwdRelTableIDs(catalog, transaction)) {
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

std::unique_ptr<NbrScanState> OnDiskGraph::prepareRelScan(TableCatalogEntry* tableEntry,
    const std::string& property) {
    return std::make_unique<OnDiskGraphNbrScanState>(context, tableEntry, graphEntry, property);
}

std::unique_ptr<NbrScanState> OnDiskGraph::prepareRelLookup(TableCatalogEntry* tableEntry) const {
    return std::make_unique<OnDiskGraphNbrScanState>(context, tableEntry, graphEntry, "",
        true /*randomLookup*/);
}

Graph::EdgeIterator OnDiskGraph::scanFwd(nodeID_t nodeID, NbrScanState& state) {
    auto& onDiskScanState = ku_dynamic_cast<OnDiskGraphNbrScanState&>(state);
    onDiskScanState.srcNodeIDVector->setValue<nodeID_t>(0, nodeID);
    onDiskScanState.dstNodeIDVector->state->getSelVectorUnsafe().setSelSize(0);
    onDiskScanState.startScan(common::RelDataDirection::FWD);
    return Graph::EdgeIterator(&onDiskScanState);
}

Graph::EdgeIterator OnDiskGraph::scanBwd(nodeID_t nodeID, NbrScanState& state) {
    auto& onDiskScanState = ku_dynamic_cast<OnDiskGraphNbrScanState&>(state);
    onDiskScanState.srcNodeIDVector->setValue<nodeID_t>(0, nodeID);
    onDiskScanState.dstNodeIDVector->state->getSelVectorUnsafe().setSelSize(0);
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

bool OnDiskGraphNbrScanState::InnerIterator::next(evaluator::ExpressionEvaluator* predicate) {
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

OnDiskGraphNbrScanState::InnerIterator::InnerIterator(const main::ClientContext* context,
    storage::RelTable* relTable, std::unique_ptr<storage::RelTableScanState> tableScanState)
    : context{context}, relTable{relTable}, tableScanState{std::move(tableScanState)} {}

void OnDiskGraphNbrScanState::InnerIterator::initScan() {
    relTable->initScanState(context->getTransaction(), *tableScanState);
}

void OnDiskGraphNbrScanState::startScan(common::RelDataDirection direction) {
    auto idx = RelDirectionUtils::relDirectionToKeyIdx(direction);
    KU_ASSERT(idx < directedIterators.size() && directedIterators[idx].getDirection() == direction);
    currentIter = &directedIterators[idx];
    currentIter->initScan();
}

bool OnDiskGraphNbrScanState::next() {
    KU_ASSERT(currentIter != nullptr);
    if (currentIter->next(relPredicateEvaluator.get())) {
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
    propertyVectors = Table::constructDataChunk(context.getMemoryManager(), std::move(types));
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

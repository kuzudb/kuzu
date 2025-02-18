#include "graph/on_disk_graph.h"

#include "binder/expression/expression_util.h"
#include "binder/expression/property_expression.h"
#include "binder/expression_visitor.h"
#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "common/assert.h"
#include "common/cast.h"
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

static expression_vector getProperties(std::shared_ptr<Expression> expr) {
    if (expr == nullptr) {
        return expression_vector{};
    }
    auto collector = PropertyExprCollector();
    collector.visit(expr);
    return ExpressionUtil::removeDuplication(collector.getPropertyExprs());
}

// We generate an empty schema with one group even if exprs is empty because we always need to
// scan edgeID and nbrNodeID which will need the state of empty data chunk.
static Schema getSchema(const expression_vector& exprs) {
    auto schema = Schema();
    schema.createGroup();
    for (auto expr : exprs) {
        schema.insertToGroupAndScope(expr, 0);
    }
    return schema;
}

static ResultSet getResultSet(Schema* schema, MemoryManager* mm) {
    auto descriptor = ResultSetDescriptor(schema);
    return ResultSet(&descriptor, mm);
}

static std::unique_ptr<ValueVector> getValueVector(const LogicalType& type, MemoryManager* mm,
    std::shared_ptr<DataChunkState> state) {
    auto vector = std::make_unique<ValueVector>(type.copy(), mm);
    vector->state = state;
    return vector;
}

OnDiskGraphNbrScanState::OnDiskGraphNbrScanState(ClientContext* context,
    TableCatalogEntry* tableEntry, std::shared_ptr<Expression> predicate)
    : OnDiskGraphNbrScanState{context, tableEntry, predicate, ""} {}

OnDiskGraphNbrScanState::OnDiskGraphNbrScanState(ClientContext* context,
    TableCatalogEntry* tableEntry, std::shared_ptr<Expression> predicate,
    const std::string& propertyName, bool randomLookup) {
    auto predicateProps = getProperties(predicate);
    auto schema = getSchema(predicateProps);
    auto mm = context->getMemoryManager();
    auto resultSet = getResultSet(&schema, mm);
    KU_ASSERT(resultSet.dataChunks.size() == 1);
    auto state = resultSet.getDataChunk(0)->state;
    srcNodeIDVector = getValueVector(LogicalType::INTERNAL_ID(), mm, state);
    srcNodeIDVector->state = DataChunkState::getSingleValueDataChunkState();
    dstNodeIDVector = getValueVector(LogicalType::INTERNAL_ID(), mm, state);
    relIDVector = getValueVector(LogicalType::INTERNAL_ID(), mm, state);
    std::optional<column_id_t> edgePropertyID;
    if (!propertyName.empty()) {
        // TODO(bmwinger): If there are both a predicate and a custom edgePropertyIndex, they will
        // currently be scanned twice. The propertyVector could simply be one of the vectors used
        // for the predicate.
        propertyVector = getValueVector(tableEntry->getProperty(propertyName).getType(), mm, state);
        edgePropertyID = tableEntry->getColumnID(propertyName);
    }
    if (predicate != nullptr) {
        auto mapper = ExpressionMapper(&schema);
        relPredicateEvaluator = mapper.getEvaluator(predicate);
        relPredicateEvaluator->init(resultSet, context);
    }
    auto table =
        context->getStorageManager()->getTable(tableEntry->getTableID())->ptrCast<RelTable>();
    for (auto dataDirection : tableEntry->ptrCast<RelTableCatalogEntry>()->getRelDataDirections()) {
        auto columnIDs = getColumnIDs(predicateProps, *tableEntry, edgePropertyID);
        auto columns = getColumns(columnIDs, *table, dataDirection);
        auto scanState = std::make_unique<RelTableScanState>(*context->getMemoryManager(),
            table->getTableID(), columnIDs, columns, table->getCSROffsetColumn(dataDirection),
            table->getCSRLengthColumn(dataDirection), dataDirection);
        // Initialize vectors to scanState
        scanState->nodeIDVector = srcNodeIDVector.get();
        scanState->outputVectors.push_back(dstNodeIDVector.get());
        scanState->outputVectors.push_back(relIDVector.get());
        for (auto& property : predicateProps) {
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
    for (auto& nodeInfo : graphEntry.nodeInfos) {
        auto id = nodeInfo.entry->getTableID();
        nodeIDToNodeTable.insert({id, storage->getTable(id)->ptrCast<NodeTable>()});
    }
    for (auto& nodeInfo : graphEntry.nodeInfos) {
        auto id = nodeInfo.entry->getTableID();
        nodeIDToNbrTableInfos.insert({id, {}});
        for (auto& relInfo : graphEntry.relInfos) {
            auto relEntry = relInfo.entry->ptrCast<RelTableCatalogEntry>();
            if (relEntry->getSrcTableID() != id) {
                continue;
            }
            auto dstEntry = catalog->getTableCatalogEntry(transaction, relEntry->getDstTableID());
            nodeIDToNbrTableInfos.at(id).emplace_back(dstEntry, relInfo.entry);
        }
    }
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

std::vector<NbrTableInfo> OnDiskGraph::getForwardNbrTableInfos(table_id_t srcNodeTableID) {
    KU_ASSERT(nodeIDToNbrTableInfos.contains(srcNodeTableID));
    return nodeIDToNbrTableInfos.at(srcNodeTableID);
}

std::unique_ptr<NbrScanState> OnDiskGraph::prepareRelScan(TableCatalogEntry* tableEntry,
    const std::string& property) {
    auto& info = graphEntry.getRelInfo(tableEntry->getTableID());
    return std::make_unique<OnDiskGraphNbrScanState>(context, tableEntry, info.predicate, property);
}

std::unique_ptr<NbrScanState> OnDiskGraph::prepareRelLookup(TableCatalogEntry* tableEntry) const {
    auto& info = graphEntry.getRelInfo(tableEntry->getTableID());
    return std::make_unique<OnDiskGraphNbrScanState>(context, tableEntry, info.predicate, "",
        true /*randomLookup*/);
}

Graph::EdgeIterator OnDiskGraph::scanFwd(nodeID_t nodeID, NbrScanState& state) {
    auto& onDiskScanState = ku_dynamic_cast<OnDiskGraphNbrScanState&>(state);
    onDiskScanState.srcNodeIDVector->setValue<nodeID_t>(0, nodeID);
    onDiskScanState.dstNodeIDVector->state->getSelVectorUnsafe().setSelSize(0);
    onDiskScanState.startScan(RelDataDirection::FWD);
    return Graph::EdgeIterator(&onDiskScanState);
}

Graph::EdgeIterator OnDiskGraph::scanBwd(nodeID_t nodeID, NbrScanState& state) {
    auto& onDiskScanState = ku_dynamic_cast<OnDiskGraphNbrScanState&>(state);
    onDiskScanState.srcNodeIDVector->setValue<nodeID_t>(0, nodeID);
    onDiskScanState.dstNodeIDVector->state->getSelVectorUnsafe().setSelSize(0);
    onDiskScanState.startScan(RelDataDirection::BWD);
    return Graph::EdgeIterator(&onDiskScanState);
}

Graph::VertexIterator OnDiskGraph::scanVertices(offset_t beginOffset, offset_t endOffsetExclusive,
    VertexScanState& state) {
    auto& onDiskVertexScanState = ku_dynamic_cast<OnDiskGraphVertexScanState&>(state);
    onDiskVertexScanState.startScan(beginOffset, endOffsetExclusive);
    return Graph::VertexIterator(&state);
}

std::unique_ptr<VertexScanState> OnDiskGraph::prepareVertexScan(TableCatalogEntry* tableEntry,
    const std::vector<std::string>& propertiesToScan) {
    return std::make_unique<OnDiskGraphVertexScanState>(*context, tableEntry, propertiesToScan);
}

bool OnDiskGraphNbrScanState::InnerIterator::next(evaluator::ExpressionEvaluator* predicate) {
    bool hasAtLeastOneSelectedValue = false;
    do {
        restoreSelVector(*tableScanState->outState);
        if (!relTable->scan(context->getTransaction(), *tableScanState)) {
            return false;
        }
        saveSelVector(*tableScanState->outState);
        if (predicate != nullptr) {
            hasAtLeastOneSelectedValue =
                predicate->select(tableScanState->outState->getSelVectorUnsafe(),
                    !tableScanState->outState->isFlat());
        } else {
            hasAtLeastOneSelectedValue = tableScanState->outState->getSelVector().getSelSize() > 0;
        }
    } while (!hasAtLeastOneSelectedValue);
    return true;
}

OnDiskGraphNbrScanState::InnerIterator::InnerIterator(const main::ClientContext* context,
    storage::RelTable* relTable, std::unique_ptr<storage::RelTableScanState> tableScanState)
    : context{context}, relTable{relTable}, tableScanState{std::move(tableScanState)} {}

void OnDiskGraphNbrScanState::InnerIterator::initScan() {
    relTable->initScanState(context->getTransaction(), *tableScanState);
}

void OnDiskGraphNbrScanState::startScan(RelDataDirection direction) {
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
    TableCatalogEntry* tableEntry, const std::vector<std::string>& propertyNames)
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

void OnDiskGraphVertexScanState::startScan(offset_t beginOffset, offset_t endOffset) {
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

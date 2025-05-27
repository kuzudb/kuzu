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
#include "storage/table/node_table.h"
#include "storage/table/rel_table.h"

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
    const TableCatalogEntry& relEntry, const std::vector<column_id_t>& propertyColumnIDs) {
    auto columnIDs = std::vector{NBR_ID_COLUMN_ID};
    for (auto columnID : propertyColumnIDs) {
        columnIDs.push_back(columnID);
    }
    for (const auto& expr : propertyExprs) {
        columnIDs.push_back(expr->constCast<PropertyExpression>().getColumnID(relEntry));
    }
    return columnIDs;
}

static expression_vector getProperties(std::shared_ptr<Expression> expr) {
    if (expr == nullptr) {
        return expression_vector{};
    }
    auto collector = PropertyExprCollector();
    collector.visit(std::move(expr));
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
    vector->state = std::move(state);
    return vector;
}

OnDiskGraphNbrScanState::OnDiskGraphNbrScanState(ClientContext* context,
    const TableCatalogEntry& entry, oid_t relTableID, std::shared_ptr<Expression> predicate)
    : OnDiskGraphNbrScanState{context, entry, relTableID, std::move(predicate), {}} {}

OnDiskGraphNbrScanState::OnDiskGraphNbrScanState(ClientContext* context,
    const TableCatalogEntry& entry, oid_t relTableID, std::shared_ptr<Expression> predicate,
    std::vector<std::string> relProperties, bool randomLookup) {
    auto predicateProps = getProperties(predicate);
    auto schema = getSchema(predicateProps);
    auto mm = context->getMemoryManager();
    auto resultSet = getResultSet(&schema, mm);
    KU_ASSERT(resultSet.dataChunks.size() == 1);
    auto state = resultSet.getDataChunk(0)->state;
    srcNodeIDVector = getValueVector(LogicalType::INTERNAL_ID(), mm, state);
    srcNodeIDVector->state = DataChunkState::getSingleValueDataChunkState();
    dstNodeIDVector = getValueVector(LogicalType::INTERNAL_ID(), mm, state);
    propertyVectors.resize(relProperties.size());
    // TODO(bmwinger): If there are both a predicate and a custom edgePropertyIndex, they will
    // currently be scanned twice. The propertyVector could simply be one of the vectors used
    // for the predicate.
    std::vector<column_id_t> relPropertyColumnIDs;
    relPropertyColumnIDs.resize(relProperties.size());
    for (auto i = 0u; i < relProperties.size(); ++i) {
        auto propertyName = relProperties[i];
        auto& property = entry.getProperty(propertyName);
        relPropertyColumnIDs[i] = entry.getColumnID(propertyName);
        KU_ASSERT(relPropertyColumnIDs[i] != INVALID_COLUMN_ID);
        propertyVectors[i] = getValueVector(property.getType(), mm, state);
    }
    if (predicate != nullptr) {
        auto mapper = ExpressionMapper(&schema);
        relPredicateEvaluator = mapper.getEvaluator(predicate);
        relPredicateEvaluator->init(resultSet, context);
    }
    auto table = context->getStorageManager()->getTable(relTableID)->ptrCast<RelTable>();
    for (auto dataDirection : entry.constCast<RelGroupCatalogEntry>().getRelDataDirections()) {
        auto columnIDs = getColumnIDs(predicateProps, entry, relPropertyColumnIDs);
        std::vector outVectors{dstNodeIDVector.get()};
        for (auto& propertyVector : propertyVectors) {
            outVectors.push_back(propertyVector.get());
        }
        for (auto& property : predicateProps) {
            auto pos = DataPos(schema.getExpressionPos(*property));
            outVectors.push_back(resultSet.getValueVector(pos).get());
        }
        auto scanState = std::make_unique<RelTableScanState>(*context->getMemoryManager(),
            srcNodeIDVector.get(), outVectors, dstNodeIDVector->state, randomLookup);
        scanState->setToTable(context->getTransaction(), table, columnIDs, {}, dataDirection);
        directedIterators.emplace_back(context, table, std::move(scanState));
    }
}

OnDiskGraph::OnDiskGraph(ClientContext* context, GraphEntry entry)
    : context{context}, graphEntry{std::move(entry)} {
    auto storage = context->getStorageManager();
    for (const auto& nodeInfo : graphEntry.nodeInfos) {
        auto id = nodeInfo.entry->getTableID();
        nodeIDToNodeTable.insert({id, storage->getTable(id)->ptrCast<NodeTable>()});
    }
    for (auto& relInfo : graphEntry.relInfos) {
        auto relGroupEntry = relInfo.entry->ptrCast<RelGroupCatalogEntry>();
        for (auto& relEntryInfo : relGroupEntry->getRelEntryInfos()) {
            auto srcTableID = relEntryInfo.nodePair.srcTableID;
            auto dstTableID = relEntryInfo.nodePair.dstTableID;
            if (!nodeIDToNodeTable.contains(srcTableID)) {
                continue;
            }
            if (!nodeIDToNodeTable.contains(dstTableID)) {
                continue;
            }
            relInfos.emplace_back(srcTableID, dstTableID, relGroupEntry, relEntryInfo.oid);
        }
    }
}

table_id_map_t<offset_t> OnDiskGraph::getMaxOffsetMap(transaction::Transaction* transaction) const {
    table_id_map_t<offset_t> result;
    for (auto tableID : getNodeTableIDs()) {
        result[tableID] = getMaxOffset(transaction, tableID);
    }
    return result;
}

offset_t OnDiskGraph::getMaxOffset(transaction::Transaction* transaction, table_id_t id) const {
    KU_ASSERT(nodeIDToNodeTable.contains(id));
    return nodeIDToNodeTable.at(id)->getNumTotalRows(transaction);
}

offset_t OnDiskGraph::getNumNodes(transaction::Transaction* transaction) const {
    offset_t numNodes = 0u;
    for (auto id : getNodeTableIDs()) {
        if (nodeOffsetMaskMap != nullptr && nodeOffsetMaskMap->containsTableID(id)) {
            numNodes += nodeOffsetMaskMap->getOffsetMask(id)->getNumMaskedNodes();
        } else {
            numNodes += getMaxOffset(transaction, id);
        }
    }
    return numNodes;
}

std::vector<GraphRelInfo> OnDiskGraph::getRelInfos(table_id_t srcTableID) {
    std::vector<GraphRelInfo> result;
    for (auto& info : relInfos) {
        if (info.srcTableID == srcTableID) {
            result.push_back(info);
        }
    }
    return result;
}

// TODO(Xiyang): since now we need to provide nbr info at prepare stage. It no longer make sense to
// have scanFwd&scanBwd. The direction has already been decided in this function.
std::unique_ptr<NbrScanState> OnDiskGraph::prepareRelScan(const TableCatalogEntry& entry,
    oid_t relTableID, table_id_t nbrTableID, std::vector<std::string> relProperties) {
    auto& info = graphEntry.getRelInfo(entry.getTableID());
    auto state = std::make_unique<OnDiskGraphNbrScanState>(context, entry, relTableID,
        info.predicate, relProperties, true /*randomLookup*/);
    if (nodeOffsetMaskMap != nullptr && nodeOffsetMaskMap->containsTableID(nbrTableID)) {
        state->nbrNodeMask = nodeOffsetMaskMap->getOffsetMask(nbrTableID);
    }
    return state;
}

Graph::EdgeIterator OnDiskGraph::scanFwd(nodeID_t nodeID, NbrScanState& state) {
    auto& onDiskScanState = ku_dynamic_cast<OnDiskGraphNbrScanState&>(state);
    onDiskScanState.srcNodeIDVector->setValue<nodeID_t>(0, nodeID);
    onDiskScanState.dstNodeIDVector->state->getSelVectorUnsafe().setSelSize(0);
    onDiskScanState.startScan(RelDataDirection::FWD);
    return EdgeIterator(&onDiskScanState);
}

Graph::EdgeIterator OnDiskGraph::scanBwd(nodeID_t nodeID, NbrScanState& state) {
    auto& onDiskScanState = ku_dynamic_cast<OnDiskGraphNbrScanState&>(state);
    onDiskScanState.srcNodeIDVector->setValue<nodeID_t>(0, nodeID);
    onDiskScanState.dstNodeIDVector->state->getSelVectorUnsafe().setSelSize(0);
    onDiskScanState.startScan(RelDataDirection::BWD);
    return EdgeIterator(&onDiskScanState);
}

Graph::VertexIterator OnDiskGraph::scanVertices(offset_t beginOffset, offset_t endOffsetExclusive,
    VertexScanState& state) {
    auto& onDiskVertexScanState = ku_dynamic_cast<OnDiskGraphVertexScanState&>(state);
    onDiskVertexScanState.startScan(beginOffset, endOffsetExclusive);
    return VertexIterator(&state);
}

std::unique_ptr<VertexScanState> OnDiskGraph::prepareVertexScan(TableCatalogEntry* tableEntry,
    const std::vector<std::string>& propertiesToScan) {
    return std::make_unique<OnDiskGraphVertexScanState>(*context, tableEntry, propertiesToScan);
}

bool OnDiskGraphNbrScanState::InnerIterator::next(evaluator::ExpressionEvaluator* predicate,
    SemiMask* nbrNodeMask_) {
    bool hasAtLeastOneSelectedValue = false;
    do {
        restoreSelVector(*tableScanState->outState);
        if (!relTable->scan(context->getTransaction(), *tableScanState)) {
            return false;
        }
        saveSelVector(*tableScanState->outState);
        hasAtLeastOneSelectedValue = tableScanState->outState->getSelVector().getSelSize() > 0;
        if (predicate != nullptr) {
            hasAtLeastOneSelectedValue =
                predicate->select(tableScanState->outState->getSelVectorUnsafe(),
                    !tableScanState->outState->isFlat());
        }
        if (nbrNodeMask_ != nullptr) {
            auto selectedSize = 0u;
            auto buffer = tableScanState->outState->getSelVectorUnsafe().getMutableBuffer();
            for (auto i = 0u; i < tableScanState->outState->getSelSize(); ++i) {
                auto pos = tableScanState->outState->getSelVector()[i];
                buffer[selectedSize] = pos;
                auto nbrNodeID = tableScanState->outputVectors[0]->getValue<nodeID_t>(pos);
                selectedSize += nbrNodeMask_->isMasked(nbrNodeID.offset);
            }
            tableScanState->outState->getSelVectorUnsafe().setToFiltered(selectedSize);
            hasAtLeastOneSelectedValue = selectedSize > 0;
        }
    } while (!hasAtLeastOneSelectedValue);
    return true;
}

OnDiskGraphNbrScanState::InnerIterator::InnerIterator(const ClientContext* context,
    RelTable* relTable, std::unique_ptr<RelTableScanState> tableScanState)
    : context{context}, relTable{relTable}, tableScanState{std::move(tableScanState)} {}

void OnDiskGraphNbrScanState::InnerIterator::initScan() const {
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
    if (currentIter->next(relPredicateEvaluator.get(), nbrNodeMask)) {
        return true;
    }
    return false;
}

OnDiskGraphVertexScanState::OnDiskGraphVertexScanState(ClientContext& context,
    const TableCatalogEntry* tableEntry, const std::vector<std::string>& propertyNames)
    : context{context}, nodeTable{ku_dynamic_cast<const NodeTable&>(
                            *context.getStorageManager()->getTable(tableEntry->getTableID()))},
      numNodesScanned{0}, currentOffset{0}, endOffsetExclusive{0} {
    std::vector<column_id_t> propertyColumnIDs;
    propertyColumnIDs.reserve(propertyNames.size());
    std::vector<LogicalType> types;
    for (const auto& property : propertyNames) {
        auto columnID = tableEntry->getColumnID(property);
        propertyColumnIDs.push_back(columnID);
        types.push_back(tableEntry->getProperty(property).getType().copy());
    }
    propertyVectors = Table::constructDataChunk(context.getMemoryManager(), std::move(types));
    nodeIDVector = std::make_unique<ValueVector>(LogicalType::INTERNAL_ID(),
        context.getMemoryManager(), propertyVectors.state);
    std::vector<ValueVector*> outVectors;
    for (auto i = 0u; i < propertyVectors.getNumValueVectors(); i++) {
        outVectors.push_back(&propertyVectors.getValueVectorMutable(i));
    }
    tableScanState =
        std::make_unique<NodeTableScanState>(nodeIDVector.get(), outVectors, propertyVectors.state);
    auto table = context.getStorageManager()->getTable(tableEntry->getTableID());
    tableScanState->setToTable(context.getTransaction(), table, propertyColumnIDs);
}

void OnDiskGraphVertexScanState::startScan(offset_t beginOffset, offset_t endOffsetExclusive) {
    numNodesScanned = 0;
    this->currentOffset = beginOffset;
    this->endOffsetExclusive = endOffsetExclusive;
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

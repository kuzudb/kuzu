#include "binder/binder.h"
#include "binder/expression/expression_util.h"
#include "binder/expression/literal_expression.h"
#include "binder/query/reading_clause/bound_table_function_call.h"
#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "catalog/hnsw_index_catalog_entry.h"
#include "common/exception/binder.h"
#include "common/mask.h"
#include "common/types/value/nested.h"
#include "expression_evaluator/expression_evaluator_utils.h"
#include "function/hnsw_index_functions.h"
#include "function/table/bind_data.h"
#include "index/hnsw_index.h"
#include "index/hnsw_index_utils.h"
#include "planner/operator/logical_hash_join.h"
#include "planner/operator/logical_table_function_call.h"
#include "planner/planner.h"
#include "processor/execution_context.h"
#include "processor/operator/table_function_call.h"
#include "storage/storage_manager.h"
#include "processor/plan_mapper.h"
#include "function/gds/gds.h"
#include "planner/operator/sip/logical_semi_masker.h"

using namespace kuzu::common;
using namespace kuzu::binder;
using namespace kuzu::function;
using namespace kuzu::planner;
using namespace kuzu::catalog;
using namespace kuzu::processor;

namespace kuzu {
namespace vector_extension {

static std::vector<LogicalType> inferInputTypes(const expression_vector& params) {
    const auto inputQueryExpression = params[2];
    std::vector<LogicalType> inputTypes;
    inputTypes.push_back(LogicalType::STRING());
    inputTypes.push_back(LogicalType::STRING());
    if (inputQueryExpression->expressionType == ExpressionType::LITERAL) {
        const auto val = inputQueryExpression->constCast<LiteralExpression>().getValue();
        inputTypes.push_back(LogicalType::ARRAY(LogicalType::FLOAT(), val.getChildrenSize()));
    } else {
        inputTypes.push_back(LogicalType::ANY());
    }
    inputTypes.push_back(LogicalType::INT64());
    return inputTypes;
}

static void validateK(int64_t val) {
    if (val <= 0) {
        throw BinderException{"The value of k must be greater than 0."};
    }
}

std::unique_ptr<TableFuncBindData> QueryHNSWIndexBindData::copy() const {
    auto bindData = std::make_unique<QueryHNSWIndexBindData>(columns);
    bindData->graphEntry = graphEntry.copy();
    bindData->nodeTableEntry = nodeTableEntry;
    bindData->indexEntry = indexEntry;
    bindData->indexColumnID = indexColumnID;
    bindData->upperHNSWRelTableEntry = upperHNSWRelTableEntry;
    bindData->lowerHNSWRelTableEntry = lowerHNSWRelTableEntry;
    bindData->config = config;
    bindData->queryExpression = queryExpression;
    bindData->kExpression = kExpression;
    bindData->outputNode = outputNode;
    return bindData;
}

static std::unique_ptr<TableFuncBindData> bindFunc(main::ClientContext* context,
    const TableFuncBindInput* input) {
    auto catalog = context->getCatalog();
    auto transaction = context->getTransaction();
    context->setUseInternalCatalogEntry(true /* useInternalCatalogEntry */);
    const auto tableOrGraphName = input->getLiteralVal<std::string>(0);
    const auto indexName = input->getLiteralVal<std::string>(1);
    // Bind graph entry and node table entry
    NodeTableCatalogEntry* nodeTableEntry = nullptr;
    graph::GraphEntry graphEntry;
    if (catalog->containsTable(transaction, tableOrGraphName)) {
        nodeTableEntry = HNSWIndexUtils::bindNodeTable(*context, tableOrGraphName, indexName,
            HNSWIndexUtils::IndexOperation::QUERY);
    } else if (context->getGraphEntrySetUnsafe().hasGraph(tableOrGraphName)) {
        graphEntry = GDSFunction::bindGraphEntry(*context, tableOrGraphName);
        if (graphEntry.nodeInfos.size() > 1 || !graphEntry.relInfos.empty()) {
            throw BinderException{stringFormat(
                "Projected graph {} either contains relationship tables or "
                "multiple node tables. Projected graphs passed to QUERY_VECTOR_INDEX function must "
                "contain only nodes from a single node table and no relationship tables.",
                tableOrGraphName)};
        }
        nodeTableEntry = graphEntry.nodeInfos[0].entry->ptrCast<NodeTableCatalogEntry>();
        HNSWIndexUtils::validateIndexExistence(*context, nodeTableEntry, indexName,
            HNSWIndexUtils::IndexOperation::QUERY);
    } else {
        throw BinderException{
            stringFormat("Cannot find table or projected graph named as {}.", tableOrGraphName)};
    }
    // Bind columns
    auto columnNames = std::vector<std::string>{QueryVectorIndexFunction::nnColumnName,
        QueryVectorIndexFunction::distanceColumnName};
    columnNames =
        TableFunction::extractYieldVariables(columnNames, input->yieldVariables);
    auto outputNode = input->binder->createQueryNode(columnNames[0], {nodeTableEntry});
    input->binder->addToScope(outputNode->toString(), outputNode);
    expression_vector columns;
    columns.push_back(outputNode->getInternalID());
    columns.push_back(input->binder->createVariable(columnNames[1], LogicalType::DOUBLE()));
    // Fill bind data
    auto bindData = std::make_unique<QueryHNSWIndexBindData>(columns);
    bindData->graphEntry = graphEntry.copy();
    bindData->nodeTableEntry = nodeTableEntry;
    auto indexEntry = catalog->getIndex(transaction, nodeTableEntry->getTableID(), indexName);
    bindData->indexEntry = indexEntry;
    KU_ASSERT(indexEntry->getPropertyIDs().size() == 1);
    auto propertyID = indexEntry->getPropertyIDs()[0];
    KU_ASSERT(nodeTableEntry->getProperty(propertyID).getType().getLogicalTypeID() == LogicalTypeID::ARRAY);
    bindData->indexColumnID = nodeTableEntry->getColumnID(propertyID);
    const auto& auxInfo = bindData->indexEntry->getAuxInfo().cast<HNSWIndexAuxInfo>();
    bindData->upperHNSWRelTableEntry = catalog->getTableCatalogEntry(transaction, auxInfo.upperRelTableID)->ptrCast<RelTableCatalogEntry>();
    bindData->lowerHNSWRelTableEntry = catalog->getTableCatalogEntry(transaction, auxInfo.lowerRelTableID)->ptrCast<RelTableCatalogEntry>();
    bindData->config = QueryHNSWConfig{input->optionalParams};
    bindData->queryExpression = input->params[2];
    bindData->kExpression = input->params[3];
    bindData->outputNode = outputNode;
    context->setUseInternalCatalogEntry(false /* useInternalCatalogEntry */);
    return bindData;
}

template<VectorElementType T>
static std::vector<T> convertQueryVector(const Value& value) {
    std::vector<T> queryVector;
    const auto numElements = value.getChildrenSize();
    queryVector.resize(numElements);
    for (auto i = 0u; i < numElements; i++) {
        queryVector[i] = NestedVal::getChildVal(&value, i)->getValue<T>();
    }
    return queryVector;
}

static Value evaluateParamExpr(std::shared_ptr<Expression> paramExpression,
    main::ClientContext* context, LogicalType expectedType) {
    std::shared_ptr<Expression> kExpr = paramExpression;
    if (paramExpression->expressionType == ExpressionType::PARAMETER) {
        kExpr = std::make_shared<LiteralExpression>(ExpressionUtil::evaluateAsLiteralValue(*kExpr),
            kExpr->getUniqueName());
    }
    Binder binder{context};
    kExpr = binder.getExpressionBinder()->implicitCastIfNecessary(kExpr, expectedType);
    return evaluator::ExpressionEvaluatorUtils::evaluateConstantExpression(kExpr, context);
}

template<typename T>
static std::vector<T> getQueryVector(main::ClientContext* context,
    std::shared_ptr<Expression> queryExpression, const LogicalType& indexType, uint64_t dimension) {
    auto value = evaluateParamExpr(queryExpression, context,
        LogicalType::ARRAY(indexType.copy(), dimension));
    KU_ASSERT(NestedVal::getChildVal(&value, 0)->getDataType() == indexType);
    return convertQueryVector<T>(value);
}

static const LogicalType& getIndexColumnType(const NodeTableCatalogEntry& nodeEntry, const IndexCatalogEntry& indexEntry) {
    const auto columnName = indexEntry.getPropertyIDs()[0];
    return nodeEntry.getProperty(columnName).getType();
}

static offset_t tableFunc(const TableFuncInput& input, TableFuncOutput& output) {
    const auto localState = input.localState->ptrCast<QueryHNSWLocalState>();
    const auto bindData = input.bindData->constPtrCast<QueryHNSWIndexBindData>();
    // As `k` can be larger than the default vector capacity, we run the actual search in the first
    // call, and output the rest of the query result in chunks in following calls.
    if (!localState->hasResultToOutput()) {
        // We start searching when there is no query result to output.
        const auto& auxInfo =
            bindData->indexEntry->getAuxInfo().cast<HNSWIndexAuxInfo>();
        const auto index = std::make_unique<OnDiskHNSWIndex>(input.context->clientContext,
            bindData->nodeTableEntry, bindData->indexColumnID,
            bindData->upperHNSWRelTableEntry, bindData->lowerHNSWRelTableEntry,
            auxInfo.config.copy());
        index->setDefaultUpperEntryPoint(auxInfo.upperEntryPoint);
        index->setDefaultLowerEntryPoint(auxInfo.lowerEntryPoint);
        const auto dimension = ArrayType::getNumElements(getIndexColumnType(*bindData->nodeTableEntry, *bindData->indexEntry));
        auto indexType = index->getElementType();
        TypeUtils::visit(
            indexType,
            [&]<VectorElementType T>(T) {
                auto queryVector = getQueryVector<T>(input.context->clientContext,
                    bindData->queryExpression, index->getElementType(), dimension);
                localState->result = index->search(input.context->clientContext->getTransaction(),
                    queryVector.data(), localState->searchState);
            },
            [&](auto) { KU_UNREACHABLE; });
    }
    KU_ASSERT(localState->result.has_value());
    if (localState->numRowsOutput >= localState->result->size()) {
        return 0;
    }
    const auto numToOutput =
        std::min(localState->result->size() - localState->numRowsOutput, DEFAULT_VECTOR_CAPACITY);
    for (auto i = 0u; i < numToOutput; i++) {
        const auto& [nodeOffset, distance] =
            localState->result.value()[i + localState->numRowsOutput];
        output.dataChunk.getValueVectorMutable(0).setValue<internalID_t>(i,
            internalID_t{nodeOffset, bindData->nodeTableEntry->getTableID()});
        output.dataChunk.getValueVectorMutable(1).setValue<double>(i, distance);
    }
    localState->numRowsOutput += numToOutput;
    output.dataChunk.state->getSelVectorUnsafe().setToUnfiltered(numToOutput);
    return numToOutput;
}

static std::unique_ptr<TableFuncSharedState> initQueryHNSWSharedState(
    const TableFuncInitSharedStateInput& input) {
    const auto bindData = input.bindData->constPtrCast<QueryHNSWIndexBindData>();
    auto context = input.context->clientContext;
    auto nodeTable = context->getStorageManager()->getTable(bindData->nodeTableEntry->getTableID())
                    ->ptrCast<storage::NodeTable>();
    auto numNodes = nodeTable->getStats(context->getTransaction()).getTableCard();
    return std::make_unique<QueryHNSWIndexSharedState>(nodeTable, numNodes);
}

std::unique_ptr<TableFuncLocalState> initQueryHNSWLocalState(
    const TableFuncInitLocalStateInput& input) {
    const auto hnswBindData = input.bindData.constPtrCast<QueryHNSWIndexBindData>();
    const auto hnswSharedState = input.sharedState.ptrCast<QueryHNSWIndexSharedState>();
    auto context = input.clientContext;
    auto val =
        evaluateParamExpr(hnswBindData->kExpression, context, LogicalType::INT64());
    auto k = ExpressionUtil::getExpressionVal<int64_t>(*hnswBindData->kExpression, val,
        LogicalType::INT64(), validateK);
    HNSWSearchState searchState{context->getTransaction(), context->getMemoryManager(),
        *hnswSharedState->nodeTable, hnswBindData->indexColumnID, hnswSharedState->numNodes,
        static_cast<uint64_t>(k), hnswBindData->config};
    const auto tableID = hnswBindData->nodeTableEntry->getTableID();
    auto& semiMasks = hnswSharedState->semiMasks;
    if (semiMasks.containsTableID(tableID)) {
        semiMasks.pin(tableID);
        searchState.semiMask = semiMasks.getPinnedMask();
    }
    return std::make_unique<QueryHNSWLocalState>(std::move(searchState));
}

static void getLogicalPlan(Planner* planner, const BoundReadingClause& readingClause,
    expression_vector predicates, LogicalPlan& plan) {
    auto& call = readingClause.constCast<BoundTableFunctionCall>();
    auto bindData = call.getBindData()->constPtrCast<QueryHNSWIndexBindData>();
    auto op = std::make_shared<LogicalTableFunctionCall>(call.getTableFunc(), bindData->copy());
    // Check if there is filter predicate on the base node table. If so, we need to plan the
    // filtered search.
    if (!bindData->graphEntry.isEmpty()) {
        KU_ASSERT(bindData->graphEntry.nodeInfos.size() == 1);
        auto& nodeInfo = bindData->graphEntry.nodeInfos[0];
        if (nodeInfo.predicate) {
            // We have filter predicate on the base node table. Should add a semi mask subplan
            // and pass the semi mask to QueryHNSWIndexFunction to perform filtered search.
            auto& nodeExpr = nodeInfo.nodeOrRel->constCast<NodeExpression>();
            auto filterPlan = planner->getNodeSemiMaskPlan(SemiMaskTargetType::SCAN_NODE, nodeExpr,
                nodeInfo.predicate);
            op->addChild(filterPlan.getLastOperator());
        }
    }
    op->computeFactorizedSchema();
    planner->planReadOp(op, predicates, plan);
    auto nodeOutput = bindData->outputNode->ptrCast<NodeExpression>();
    KU_ASSERT(nodeOutput != nullptr);
    auto scanPlan = planner->getNodePropertyScanPlan(*nodeOutput);
    if (scanPlan.isEmpty()) {
        return;
    }
    expression_vector joinConditions;
    joinConditions.push_back(nodeOutput->getInternalID());
    planner->appendHashJoin(joinConditions, JoinType::INNER, scanPlan, plan, plan);
    plan.getLastOperator()->cast<LogicalHashJoin>().getSIPInfoUnsafe().direction =
        SIPDirection::FORCE_BUILD_TO_PROBE;
}

static std::unique_ptr<PhysicalOperator> getPhysicalPlan(PlanMapper* planMapper,
    const LogicalOperator* logicalOp) {
    auto op = TableFunction::getPhysicalPlan(planMapper, logicalOp);
    auto sharedState = op->constCast<TableFunctionCall>().getSharedState();
    auto bindData = logicalOp->constPtrCast<LogicalTableFunctionCall>()->getBindData()->constPtrCast<QueryHNSWIndexBindData>();
    // Map node predicate pipeline
    if (!bindData->graphEntry.isEmpty() && bindData->graphEntry.nodeInfos[0].predicate) {
        sharedState->semiMasks.addMask(bindData->nodeTableEntry->getTableID(),
                SemiMaskUtil::createMask(bindData->numRows));
        planMapper->addOperatorMapping(logicalOp, op.get());
        KU_ASSERT(logicalOp->getNumChildren() == 1);
        auto logicalRoot = logicalOp->getChild(0);
        KU_ASSERT(logicalRoot->getNumChildren() == 1);
        auto child = logicalRoot->getChild(0);
        KU_ASSERT(child->getOperatorType() == LogicalOperatorType::SEMI_MASKER);
        auto logicalSemiMasker = child->ptrCast<LogicalSemiMasker>();
        logicalSemiMasker->addTarget(logicalOp);
        auto root = planMapper->mapOperator(logicalRoot.get());
        op->addChild(std::move(root));
        planMapper->eraseOperatorMapping(logicalOp);
    }
    return op;
}

function_set QueryVectorIndexFunction::getFunctionSet() {
    function_set functionSet;
    std::vector inputTypes{LogicalTypeID::STRING, LogicalTypeID::STRING, LogicalTypeID::ARRAY,
        LogicalTypeID::INT64};
    auto tableFunction = std::make_unique<TableFunction>(name, inputTypes);
    tableFunction->tableFunc = tableFunc;
    tableFunction->bindFunc = bindFunc;
    tableFunction->initSharedStateFunc = initQueryHNSWSharedState;
    tableFunction->initLocalStateFunc = initQueryHNSWLocalState;
    tableFunction->canParallelFunc = [] { return false; };
    tableFunction->getLogicalPlanFunc = getLogicalPlan;
    tableFunction->getPhysicalPlanFunc = getPhysicalPlan;
    tableFunction->inferInputTypes = inferInputTypes;
    functionSet.push_back(std::move(tableFunction));
    return functionSet;
}

} // namespace vector_extension
} // namespace kuzu

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
#include "graph/graph_entry_set.h"
#include "index/hnsw_index.h"
#include "index/hnsw_index_utils.h"
#include "parser/parser.h"
#include "planner/operator/logical_hash_join.h"
#include "planner/operator/logical_projection.h"
#include "planner/operator/logical_table_function_call.h"
#include "planner/operator/sip/logical_semi_masker.h"
#include "planner/planner.h"
#include "processor/execution_context.h"
#include "processor/operator/table_function_call.h"
#include "processor/plan_mapper.h"
#include "storage/storage_manager.h"

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
    bindData->nodeTableEntry = nodeTableEntry;
    bindData->indexEntry = indexEntry;
    bindData->indexColumnID = indexColumnID;
    bindData->config = config;
    bindData->queryExpression = queryExpression;
    bindData->kExpression = kExpression;
    bindData->outputNode = outputNode;
    bindData->filterStatement = filterStatement;
    return bindData;
}

static std::unique_ptr<TableFuncBindData> bindFunc(main::ClientContext* context,
    const TableFuncBindInput* input) {
    auto catalog = context->getCatalog();
    auto transaction = context->getTransaction();
    context->setUseInternalCatalogEntry(true /* useInternalCatalogEntry */);
    const auto tableOrGraphName = input->getLiteralVal<std::string>(0);
    auto indexName = input->getLiteralVal<std::string>(1);
    // Bind graph entry and node table entry
    std::string tableName;
    std::unique_ptr<BoundStatement> boundStatement;
    if (catalog->containsTable(transaction, tableOrGraphName)) {
        tableName = tableOrGraphName;
    } else if (context->getGraphEntrySetUnsafe().hasGraph(tableOrGraphName)) {
        auto graphEntry = context->getGraphEntrySetUnsafe().getEntry(tableOrGraphName);
        std::string cypherQuery;
        if (graphEntry->type == graph::GraphEntryType::NATIVE) {
            auto& nativeEntry = graphEntry->cast<graph::ParsedNativeGraphEntry>();
            if (!nativeEntry.relInfos.empty() || nativeEntry.nodeInfos.size() != 1) {
                throw BinderException(stringFormat("In vector filtered search, projected graph {} "
                                                   "must contain exactly one node table.",
                    tableOrGraphName));
            }
            auto nodeInfo = nativeEntry.nodeInfos[0];
            cypherQuery = stringFormat("MATCH (n:`{}`) WHERE {} RETURN n", nodeInfo.tableName,
                nodeInfo.predicate);
        } else {
            KU_ASSERT(graphEntry->type == graph::GraphEntryType::CYPHER);
            cypherQuery = graphEntry->cast<graph::ParsedCypherGraphEntry>().cypherQuery;
        }
        try {
            auto parsedStatements = parser::Parser::parseQuery(cypherQuery);
            KU_ASSERT(parsedStatements.size() == 1);
            auto binder = Binder(context);
            boundStatement = binder.bind(*parsedStatements[0]);
        } catch (Exception& e) {
            // LCOV_EXCL_START
            throw BinderException{
                stringFormat("Failed to bind the filter query. Found error: {}", e.what())};
            // LCOV_EXCL_STOP
        }
        auto resultColumns = boundStatement->getStatementResult()->getColumns();
        if (resultColumns.size() != 1) {
            throw BinderException(
                stringFormat("The return clause of a filter query should contain "
                             "exactly one node expression. Found more than one expressions: {}.",
                    ExpressionUtil::toString(resultColumns)));
        }
        auto resultColumn = resultColumns[0];
        if (resultColumn->getDataType().getLogicalTypeID() != LogicalTypeID::NODE) {
            throw BinderException(stringFormat("The return clause of a filter query should be of "
                                               "type NODE. Found type {} instead.",
                resultColumn->getDataType().toString()));
        }
        auto& node = resultColumn->constCast<NodeExpression>();
        if (node.getNumEntries() != 1) {
            throw BinderException(stringFormat(
                "Node {} in the return clause of the filter query should have one label.",
                node.toString()));
        }
        tableName = node.getEntry(0)->getName();
    } else {
        throw BinderException(
            stringFormat("Cannot find table or graph named as {}.", tableOrGraphName));
    }
    auto nodeTableEntry = HNSWIndexUtils::bindNodeTable(*context, tableName, indexName,
        HNSWIndexUtils::IndexOperation::QUERY);
    // Bind columns
    auto columnNames = std::vector<std::string>{QueryVectorIndexFunction::nnColumnName,
        QueryVectorIndexFunction::distanceColumnName};
    columnNames = TableFunction::extractYieldVariables(columnNames, input->yieldVariables);
    auto outputNode = input->binder->createQueryNode(columnNames[0], {nodeTableEntry});
    input->binder->addToScope(outputNode->toString(), outputNode);
    expression_vector columns;
    columns.push_back(outputNode->getInternalID());
    columns.push_back(input->binder->createVariable(columnNames[1], LogicalType::DOUBLE()));
    // Fill bind data
    auto bindData = std::make_unique<QueryHNSWIndexBindData>(columns);
    bindData->nodeTableEntry = nodeTableEntry;
    auto indexEntry = catalog->getIndex(transaction, nodeTableEntry->getTableID(), indexName);
    bindData->indexEntry = indexEntry;
    KU_ASSERT(indexEntry->getPropertyIDs().size() == 1);
    auto propertyID = indexEntry->getPropertyIDs()[0];
    KU_ASSERT(nodeTableEntry->getProperty(propertyID).getType().getLogicalTypeID() ==
              LogicalTypeID::ARRAY);
    bindData->indexColumnID = nodeTableEntry->getColumnID(propertyID);
    bindData->queryExpression = input->params[2];
    bindData->kExpression = input->params[3];
    bindData->outputNode = outputNode;
    bindData->config = QueryHNSWConfig{input->optionalParams};
    bindData->filterStatement = std::move(boundStatement);
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
    main::ClientContext* context, const LogicalType& expectedType) {
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

static const LogicalType& getIndexColumnType(const NodeTableCatalogEntry& nodeEntry,
    const IndexCatalogEntry& indexEntry) {
    const auto columnName = indexEntry.getPropertyIDs()[0];
    return nodeEntry.getProperty(columnName).getType();
}

// This struct wraps a vector of embedding data
// It exists so that we can match the interface for on-disk HNSW search
template<typename T>
struct HNSWQueryVector : GetEmbeddingsScanState {
    HNSWQueryVector(main::ClientContext* context, std::shared_ptr<Expression> queryExpression,
        const LogicalType& indexType, uint64_t dimension)
        : data(getQueryVector<T>(context, std::move(queryExpression), indexType, dimension)) {}

    void* getEmbeddingPtr([[maybe_unused]] const EmbeddingHandle& handle) override {
        KU_ASSERT(!handle.isNull());
        KU_ASSERT(handle.offsetInData == 0);
        return reinterpret_cast<void*>(data.data());
    }

    void addEmbedding(const EmbeddingHandle&) override {};
    void reclaimEmbedding(const EmbeddingHandle&) override {};

    std::vector<T> data;
};

static offset_t tableFunc(const TableFuncInput& input, TableFuncOutput& output) {
    const auto localState = input.localState->ptrCast<QueryHNSWLocalState>();
    const auto bindData = input.bindData->constPtrCast<QueryHNSWIndexBindData>();
    // As `k` can be larger than the default vector capacity, we run the actual search in the first
    // call, and output the rest of the query result in chunks in the following calls.
    if (!localState->hasResultToOutput()) {
        const auto nodeTable = input.context->clientContext->getStorageManager()
                                   ->getTable(bindData->nodeTableEntry->getTableID())
                                   ->ptrCast<storage::NodeTable>();
        auto indexOpt = nodeTable->getIndex(bindData->indexEntry->getIndexName());
        KU_ASSERT(indexOpt.has_value());
        auto& index = indexOpt.value()->cast<OnDiskHNSWIndex>();
        const auto dimension = ArrayType::getNumElements(
            getIndexColumnType(*bindData->nodeTableEntry, *bindData->indexEntry));
        auto indexType = index.getElementType();
        TypeUtils::visit(
            indexType,
            [&]<VectorElementType T>(T) {
                auto queryVector = HNSWQueryVector<T>(input.context->clientContext,
                    bindData->queryExpression, index.getElementType(), dimension);
                auto queryVectorHandle = EmbeddingHandle{0, &queryVector};
                localState->result = index.search(input.context->clientContext->getTransaction(),
                    queryVectorHandle, localState->searchState);
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
    auto nodeTable = context->getStorageManager()
                         ->getTable(bindData->nodeTableEntry->getTableID())
                         ->ptrCast<storage::NodeTable>();
    auto numNodes = nodeTable->getStats(context->getTransaction()).getTableCard();
    return std::make_unique<QueryHNSWIndexSharedState>(nodeTable, numNodes);
}

std::unique_ptr<TableFuncLocalState> initQueryHNSWLocalState(
    const TableFuncInitLocalStateInput& input) {
    const auto hnswBindData = input.bindData.constPtrCast<QueryHNSWIndexBindData>();
    const auto hnswSharedState = input.sharedState.ptrCast<QueryHNSWIndexSharedState>();
    auto context = input.clientContext;
    auto val = evaluateParamExpr(hnswBindData->kExpression, context, LogicalType::INT64());
    auto k = ExpressionUtil::getExpressionVal<int64_t>(*hnswBindData->kExpression, val,
        LogicalType::INT64(), validateK);
    auto upperRelTableName = HNSWIndexUtils::getUpperGraphTableName(
        hnswBindData->nodeTableEntry->getTableID(), hnswBindData->indexEntry->getIndexName());
    auto lowerRelTableName = HNSWIndexUtils::getLowerGraphTableName(
        hnswBindData->nodeTableEntry->getTableID(), hnswBindData->indexEntry->getIndexName());
    auto catalog = context->getCatalog();
    auto upperRelTableEntry =
        catalog->getTableCatalogEntry(context->getTransaction(), upperRelTableName, true)
            ->ptrCast<RelGroupCatalogEntry>();
    auto lowerRelTableEntry =
        catalog->getTableCatalogEntry(context->getTransaction(), lowerRelTableName, true)
            ->ptrCast<RelGroupCatalogEntry>();
    HNSWSearchState searchState{context, hnswBindData->nodeTableEntry, upperRelTableEntry,
        lowerRelTableEntry, *hnswSharedState->nodeTable, hnswBindData->indexColumnID,
        hnswSharedState->numNodes, static_cast<uint64_t>(k), hnswBindData->config};
    const auto tableID = hnswBindData->nodeTableEntry->getTableID();
    auto& semiMasks = hnswSharedState->semiMasks;
    if (semiMasks.containsTableID(tableID)) {
        semiMasks.pin(tableID);
        searchState.semiMask = semiMasks.getPinnedMask();
    }
    return std::make_unique<QueryHNSWLocalState>(std::move(searchState));
}

static void getLogicalPlan(Planner* planner, const BoundReadingClause& readingClause,
    const expression_vector& predicates, LogicalPlan& plan) {
    auto& call = readingClause.constCast<BoundTableFunctionCall>();
    auto bindData = call.getBindData()->constPtrCast<QueryHNSWIndexBindData>();
    auto op = std::make_shared<LogicalTableFunctionCall>(call.getTableFunc(), bindData->copy());
    if (bindData->filterStatement != nullptr) {
        auto& node = bindData->filterStatement->getSingleColumnExpr()->constCast<NodeExpression>();
        auto filterPlan = planner->planStatement(*bindData->filterStatement);
        auto root = filterPlan.getLastOperator();
        KU_ASSERT(root->getOperatorType() == LogicalOperatorType::PROJECTION);
        auto projection = root->ptrCast<LogicalProjection>();
        KU_ASSERT(projection->getExpressionsToProject().size() == 1);
        auto expr = projection->getExpressionsToProject()[0];
        KU_ASSERT(expr->getDataType().getLogicalTypeID() == LogicalTypeID::NODE);
        auto nodeID = expr->constCast<NodeExpression>().getInternalID();
        projection->setExpressionsToProject({nodeID});
        // Pre-append semi mask before projection
        filterPlan.setLastOperator(projection->getChild(0));
        planner->appendNodeSemiMask(SemiMaskTargetType::SCAN_NODE, node, filterPlan);
        auto& semiMasker = filterPlan.getLastOperator()->cast<LogicalSemiMasker>();
        semiMasker.addTarget(op.get());
        projection->setChild(0, filterPlan.getLastOperator());
        projection->computeFactorizedSchema();
        filterPlan.setLastOperator(root);
        planner->appendDummySink(filterPlan);
        op->addChild(filterPlan.getLastOperator());
    }
    op->computeFactorizedSchema();
    planner->planReadOp(op, predicates, plan);
    auto nodeOutput = bindData->outputNode->ptrCast<NodeExpression>();
    KU_ASSERT(nodeOutput != nullptr);
    planner->getCardinliatyEstimatorUnsafe().init(*nodeOutput);
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
    auto bindData = logicalOp->constPtrCast<LogicalTableFunctionCall>()
                        ->getBindData()
                        ->constPtrCast<QueryHNSWIndexBindData>();
    // Map node predicate pipeline
    if (bindData->filterStatement != nullptr) {
        sharedState->semiMasks.addMask(bindData->nodeTableEntry->getTableID(),
            SemiMaskUtil::createMask(bindData->numRows));
        planMapper->addOperatorMapping(logicalOp, op.get());
        KU_ASSERT(logicalOp->getNumChildren() == 1);
        auto logicalRoot = logicalOp->getChild(0);
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

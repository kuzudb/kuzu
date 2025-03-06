#include "binder/binder.h"
#include "binder/expression/expression_util.h"
#include "binder/expression/literal_expression.h"
#include "binder/query/reading_clause/bound_table_function_call.h"
#include "catalog/catalog_entry/hnsw_index_catalog_entry.h"
#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "common/exception/binder.h"
#include "common/mask.h"
#include "common/types/value/nested.h"
#include "expression_evaluator/expression_evaluator_utils.h"
#include "function/table/bind_data.h"
#include "function/table/hnsw/hnsw_index_functions.h"
#include "planner/operator/logical_hash_join.h"
#include "planner/operator/logical_table_function_call.h"
#include "planner/planner.h"
#include "processor/execution_context.h"
#include "storage/index/hnsw_index.h"
#include "storage/index/hnsw_index_utils.h"
#include "storage/index/index_utils.h"
#include "storage/storage_manager.h"

namespace kuzu {
namespace function {

QueryHNSWIndexBindData::QueryHNSWIndexBindData(main::ClientContext* context,
    binder::expression_vector columns, BoundQueryHNSWIndexInput boundInput,
    storage::QueryHNSWConfig config, std::shared_ptr<binder::NodeExpression> outputNode)
    : TableFuncBindData{std::move(columns), 1 /*maxOffset*/}, context{context},
      boundInput{std::move(boundInput)}, config{config}, outputNode{std::move(outputNode)} {
    const auto indexEntry = this->boundInput.indexEntry;
    auto& indexAuxInfo = indexEntry->getAuxInfo().cast<catalog::HNSWIndexAuxInfo>();
    indexColumnID = this->boundInput.nodeTableEntry->getColumnID(indexEntry->getPropertyIDs()[0]);
    const auto catalog = context->getCatalog();
    upperHNSWRelTableEntry =
        catalog->getTableCatalogEntry(context->getTransaction(), indexAuxInfo.upperRelTableID)
            ->ptrCast<catalog::RelTableCatalogEntry>();
    lowerHNSWRelTableEntry =
        catalog->getTableCatalogEntry(context->getTransaction(), indexAuxInfo.lowerRelTableID)
            ->ptrCast<catalog::RelTableCatalogEntry>();
}

static std::vector<common::LogicalType> inferInputTypes(const binder::expression_vector& params) {
    auto inputQueryExpression = params[2];
    std::vector<common::LogicalType> inputTypes;
    inputTypes.push_back(common::LogicalType::STRING());
    inputTypes.push_back(common::LogicalType::STRING());
    if (inputQueryExpression->expressionType == common::ExpressionType::LITERAL) {
        auto val = inputQueryExpression->constCast<binder::LiteralExpression>().getValue();
        inputTypes.push_back(
            common::LogicalType::ARRAY(common::LogicalType::FLOAT(), val.getChildrenSize()));
    } else {
        inputTypes.push_back(common::LogicalType::ANY());
    }
    inputTypes.push_back(common::LogicalType::INT64());
    return inputTypes;
}

static std::unique_ptr<TableFuncBindData> bindFunc(main::ClientContext* context,
    const TableFuncBindInput* input) {
    context->setUseInternalCatalogEntry(true /* useInternalCatalogEntry */);
    const auto tableOrGraphName = input->getLiteralVal<std::string>(0);
    const auto indexName = input->getLiteralVal<std::string>(1);
    auto inputQueryExpression = input->params[2];
    const auto k = input->getLiteralVal<int64_t>(3);

    catalog::NodeTableCatalogEntry* nodeTableEntry = nullptr;
    const graph::GraphEntry* graphEntry = nullptr;
    if (context->getCatalog()->containsTable(context->getTransaction(), tableOrGraphName)) {
        nodeTableEntry = storage::IndexUtils::bindNodeTable(*context, tableOrGraphName, indexName,
            storage::IndexOperation::QUERY);
    } else if (context->getGraphEntrySetUnsafe().hasGraph(tableOrGraphName)) {
        graphEntry = &context->getGraphEntrySetUnsafe().getEntry(tableOrGraphName);
        if (graphEntry->nodeInfos.size() > 1 || !graphEntry->relInfos.empty()) {
            throw common::BinderException{common::stringFormat(
                "Projected graph {} either contains relationship tables or "
                "multiple node tables. Projected graphs passed to QUERY_HNSW_INDEX function must "
                "contain only nodes from a single node table and no relationship tables.",
                tableOrGraphName)};
        }
        nodeTableEntry = graphEntry->nodeInfos[0].entry->ptrCast<catalog::NodeTableCatalogEntry>();
        storage::IndexUtils::validateIndexExistence(*context, nodeTableEntry, indexName,
            storage::IndexOperation::QUERY);
    } else {
        throw common::BinderException{common::stringFormat(
            "Cannot find table or projected graph named as {}.", tableOrGraphName)};
    }
    const auto indexEntry = context->getCatalog()->getIndex(context->getTransaction(),
        nodeTableEntry->getTableID(), indexName);
    const auto& auxInfo = indexEntry->getAuxInfo().cast<catalog::HNSWIndexAuxInfo>();
    KU_ASSERT(
        nodeTableEntry->getProperty(indexEntry->getPropertyIDs()[0]).getType().getLogicalTypeID() ==
        common::LogicalTypeID::ARRAY);
    KU_UNUSED(auxInfo);

    auto outputNode = input->binder->createQueryNode("nn", {nodeTableEntry});
    input->binder->addToScope(outputNode->toString(), outputNode);
    binder::expression_vector columns;
    columns.push_back(outputNode->getInternalID());
    columns.push_back(input->binder->createVariable("_distance", common::LogicalType::DOUBLE()));
    auto boundInput = BoundQueryHNSWIndexInput{nodeTableEntry, graphEntry, indexEntry,
        std::move(inputQueryExpression), static_cast<uint64_t>(k)};
    auto config = storage::QueryHNSWConfig{input->optionalParams};
    return std::make_unique<QueryHNSWIndexBindData>(context, std::move(columns), boundInput, config,
        outputNode);
}

static std::vector<float> convertQueryVector(const common::Value& value) {
    std::vector<float> queryVector;
    auto numElements = value.getChildrenSize();
    queryVector.reserve(numElements);
    for (auto i = 0u; i < numElements; i++) {
        queryVector[i] = common::NestedVal::getChildVal(&value, i)->getValue<float>();
    }
    return queryVector;
}

static std::vector<float> getQueryVector(main::ClientContext* context,
    std::shared_ptr<binder::Expression> queryExpression, uint64_t dimension) {
    std::shared_ptr<binder::Expression> literalExpression = queryExpression;
    if (queryExpression->expressionType == common::ExpressionType::PARAMETER) {
        literalExpression = std::make_shared<binder::LiteralExpression>(
            binder::ExpressionUtil::evaluateAsLiteralValue(*queryExpression),
            queryExpression->getUniqueName());
    }
    binder::Binder binder{context};
    auto expr = binder.getExpressionBinder()->implicitCastIfNecessary(literalExpression,
        common::LogicalType::ARRAY(common::LogicalType::FLOAT(), dimension));
    auto value = evaluator::ExpressionEvaluatorUtils::evaluateConstantExpression(expr, context);
    KU_ASSERT(
        common::NestedVal::getChildVal(&value, 0)->getDataType() == common::LogicalType::FLOAT());
    return convertQueryVector(value);
}

static const common::LogicalType& getIndexColumnType(const BoundQueryHNSWIndexInput& boundInput) {
    const auto columnName = boundInput.indexEntry->getPropertyIDs()[0];
    return boundInput.nodeTableEntry->getProperty(columnName).getType();
}

static common::offset_t tableFunc(const TableFuncInput& input, TableFuncOutput& output) {
    const auto localState = input.localState->ptrCast<QueryHNSWLocalState>();
    const auto bindData = input.bindData->constPtrCast<QueryHNSWIndexBindData>();
    // As `k` can be larger than the default vector capacity, we run the actual search in the first
    // call, and output the rest of the query result in chunks in following calls.
    if (!localState->hasResultToOutput()) {
        // We start searching when there is no query result to output.
        const auto& auxInfo =
            bindData->boundInput.indexEntry->getAuxInfo().cast<catalog::HNSWIndexAuxInfo>();
        const auto index = std::make_unique<storage::OnDiskHNSWIndex>(input.context->clientContext,
            bindData->boundInput.nodeTableEntry, bindData->indexColumnID,
            bindData->upperHNSWRelTableEntry, bindData->lowerHNSWRelTableEntry,
            auxInfo.config.copy());
        index->setDefaultUpperEntryPoint(auxInfo.upperEntryPoint);
        index->setDefaultLowerEntryPoint(auxInfo.lowerEntryPoint);
        const auto dimension =
            common::ArrayType::getNumElements(getIndexColumnType(bindData->boundInput));
        auto queryVector = getQueryVector(input.context->clientContext,
            bindData->boundInput.queryExpression, dimension);
        localState->result = index->search(input.context->clientContext->getTransaction(),
            queryVector, localState->searchState);
    }
    KU_ASSERT(localState->result.has_value());
    if (localState->numRowsOutput >= localState->result->size()) {
        return 0;
    }
    const auto numToOutput = std::min(localState->result->size() - localState->numRowsOutput,
        common::DEFAULT_VECTOR_CAPACITY);
    for (auto i = 0u; i < numToOutput; i++) {
        const auto& [nodeOffset, distance] =
            localState->result.value()[i + localState->numRowsOutput];
        output.dataChunk.getValueVectorMutable(0).setValue<common::internalID_t>(i,
            common::internalID_t{nodeOffset, bindData->boundInput.nodeTableEntry->getTableID()});
        output.dataChunk.getValueVectorMutable(1).setValue<double>(i, distance);
    }
    localState->numRowsOutput += numToOutput;
    output.dataChunk.state->getSelVectorUnsafe().setToUnfiltered(numToOutput);
    return numToOutput;
}

QueryHNSWIndexSharedState::QueryHNSWIndexSharedState(const QueryHNSWIndexBindData& bindData)
    : TableFuncSharedState{1 /* maxOffset */} {
    const auto storageManager = bindData.context->getStorageManager();
    nodeTable = storageManager->getTable(bindData.boundInput.nodeTableEntry->getTableID())
                    ->ptrCast<storage::NodeTable>();
    numNodes = nodeTable->getStats(bindData.context->getTransaction()).getTableCard();
    if (bindData.boundInput.graphEntry) {
        KU_ASSERT(!bindData.boundInput.graphEntry->nodeInfos.empty());
        if (bindData.boundInput.graphEntry->nodeInfos[0].predicate) {
            semiMasks.addMask(bindData.boundInput.nodeTableEntry->getTableID(),
                common::SemiMaskUtil::createMask(numNodes));
        }
    }
}

static std::unique_ptr<TableFuncSharedState> initQueryHNSWSharedState(
    const TableFuncInitSharedStateInput& input) {
    const auto bindData = input.bindData->constPtrCast<QueryHNSWIndexBindData>();
    return std::make_unique<QueryHNSWIndexSharedState>(*bindData);
}

std::unique_ptr<TableFuncLocalState> initQueryHNSWLocalState(
    const TableFuncInitLocalStateInput& input) {
    const auto hnswBindData = input.bindData.constPtrCast<QueryHNSWIndexBindData>();
    const auto hnswSharedState = input.sharedState.ptrCast<QueryHNSWIndexSharedState>();
    auto context = input.clientContext;
    storage::HNSWSearchState searchState{context->getTransaction(), context->getMemoryManager(),
        *hnswSharedState->nodeTable, hnswBindData->indexColumnID, hnswSharedState->numNodes,
        hnswBindData->boundInput.k, hnswBindData->config};
    const auto tableID = hnswBindData->boundInput.nodeTableEntry->getTableID();
    auto& semiMasks = hnswSharedState->semiMasks;
    if (semiMasks.containsTableID(tableID)) {
        semiMasks.pin(tableID);
        searchState.semiMask = semiMasks.getPinnedMask();
    }
    return std::make_unique<QueryHNSWLocalState>(std::move(searchState));
}

static void getLogicalPlan(planner::Planner* planner,
    const binder::BoundReadingClause& readingClause,
    std::shared_ptr<planner::LogicalOperator> logicalOp,
    const std::vector<std::unique_ptr<planner::LogicalPlan>>& logicalPlans) {
    auto& call = readingClause.constCast<binder::BoundTableFunctionCall>();
    const auto bindData = call.getBindData()->constPtrCast<QueryHNSWIndexBindData>();
    const auto callOp = logicalOp->ptrCast<planner::LogicalTableFunctionCall>();
    binder::expression_vector predicatesToPull;
    binder::expression_vector predicatesToPush;
    planner::Planner::splitPredicates(call.getBindData()->columns, call.getConjunctivePredicates(),
        predicatesToPull, predicatesToPush);
    for (auto& plan : logicalPlans) {
        // Plan the QueryHNSWIndexFunction operator itself and post filters if applicable.
        planner->planReadOp(logicalOp, predicatesToPush, *plan);
        if (!predicatesToPull.empty()) {
            planner->appendFilters(predicatesToPull, *plan);
        }
        auto& node = callOp->getBindData()->getNodeOutput()->constCast<binder::NodeExpression>();
        // Check if there is filter predicate on the base node table. If so, we need to plan the
        // filtered search.
        if (bindData->boundInput.graphEntry) {
            KU_ASSERT(bindData->boundInput.graphEntry->nodeInfos.size() == 1);
            auto& nodeInfo = bindData->boundInput.graphEntry->nodeInfos[0];
            if (nodeInfo.predicate) {
                // We have filter predicate on the base node table. Should add a semi mask subplan
                // and pass the semi mask to QueryHNSWIndexFunction to perform filtered search.
                auto& nodeExpr = nodeInfo.nodeOrRel->constCast<binder::NodeExpression>();
                auto filterPlan = planner->planNodeSemiMask(planner::SemiMaskTargetType::SCAN_NODE,
                    nodeExpr, nodeInfo.predicate);
                std::vector nodeMaskRoots{filterPlan.getLastOperator()};
                callOp->setNodeMaskRoots(std::move(nodeMaskRoots));
                callOp->addChild(filterPlan.getLastOperator());
            }
        }
        // Plan the final hash join with the base node table to scan properties from the base table.
        auto properties = planner->getProperties(node);
        planner->getCardinalityEstimator().addNodeIDDomAndStats(
            planner->clientContext->getTransaction(), *node.getInternalID(), node.getTableIDs());
        auto scanPlan = planner::LogicalPlan();
        planner->appendScanNodeTable(node.getInternalID(), node.getTableIDs(), properties,
            scanPlan);
        binder::expression_vector joinConditions;
        joinConditions.push_back(node.getInternalID());
        planner->appendHashJoin(joinConditions, common::JoinType::INNER, scanPlan, *plan, *plan);
        plan->getLastOperator()->cast<planner::LogicalHashJoin>().getSIPInfoUnsafe().direction =
            planner::SIPDirection::FORCE_BUILD_TO_PROBE;
    }
}

function_set QueryHNSWIndexFunction::getFunctionSet() {
    function_set functionSet;
    std::vector inputTypes{common::LogicalTypeID::STRING, common::LogicalTypeID::STRING,
        common::LogicalTypeID::ARRAY, common::LogicalTypeID::INT64};
    auto tableFunction = std::make_unique<TableFunction>("query_hnsw_index", inputTypes);
    tableFunction->tableFunc = tableFunc;
    tableFunction->bindFunc = bindFunc;
    tableFunction->initSharedStateFunc = initQueryHNSWSharedState;
    tableFunction->initLocalStateFunc = initQueryHNSWLocalState;
    tableFunction->canParallelFunc = [] { return false; };
    tableFunction->getLogicalPlanFunc = getLogicalPlan;
    tableFunction->inferInputTypes = inferInputTypes;
    functionSet.push_back(std::move(tableFunction));
    return functionSet;
}

} // namespace function
} // namespace kuzu

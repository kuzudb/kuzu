#include "binder/bound_update_vector_index.h"
#include "binder/ddl/bound_create_vector_index.h"
#include "binder/expression/property_expression.h"
#include "planner/operator/ddl/logical_create_vector_index.h"
#include "planner/operator/logical_update_vector_index.h"
#include "planner/planner.h"
#include "main/client_context.h"
#include "storage/storage_manager.h"

using namespace kuzu::binder;

namespace kuzu {
namespace planner {

void Planner::appendCreateVectorIndex(const BoundStatement& statement, LogicalPlan& plan) {
    auto& createVectorIndex =
        ku_dynamic_cast<const BoundStatement&, const BoundCreateVectorIndex&>(statement);
    auto tableEntry = createVectorIndex.getTableEntry();
    auto embeddingProperty = tableEntry->getProperty(createVectorIndex.getPropertyID());

    // Append the create vector index node
    auto op = make_shared<LogicalCreateVectorIndex>(tableEntry->getName(),
        embeddingProperty->getName(), tableEntry->getTableID(), embeddingProperty->getPropertyID(),
        createVectorIndex.getVectorIndexConfig(), statement.getStatementResult()->getSingleColumnExpr());
    plan.setLastOperator(std::move(op));
}

void Planner::appendUpdateVectorIndex(const binder::BoundStatement& statement, LogicalPlan& plan) {
    auto outExprs = statement.getStatementResult()->getColumns();
    auto& updateVectorIndex =
        ku_dynamic_cast<const BoundStatement&, const BoundUpdateVectorIndex&>(statement);
    auto tableEntry = updateVectorIndex.getTableEntry();

    common::table_id_map_t<SingleLabelPropertyInfo> internalIdInfo;
    internalIdInfo.insert(
        {tableEntry->getTableID(), SingleLabelPropertyInfo(false, INVALID_PROPERTY_ID)});
    auto offset = std::make_shared<PropertyExpression>(LogicalType::INTERNAL_ID(),
        InternalKeyword::ID, InternalKeyword::ID, InternalKeyword::ID, std::move(internalIdInfo));
    cardinalityEstimator.addNodeIDDom(*offset, {tableEntry->getTableID()}, clientContext->getTx());

    auto embeddingProperty = tableEntry->getProperty(updateVectorIndex.getPropertyID());
    std::unordered_map<table_id_t, SingleLabelPropertyInfo> embeddingInfo;
    embeddingInfo.insert({tableEntry->getTableID(),
        SingleLabelPropertyInfo(false, embeddingProperty->getPropertyID())});
    auto embedding = std::make_shared<PropertyExpression>(embeddingProperty->getDataType().copy(),
        embeddingProperty->getName(), embeddingProperty->getName(), embeddingProperty->getName(),
        std::move(embeddingInfo));

    auto header = clientContext->getStorageManager()->getVectorIndexHeaderReadOnlyVersion(tableEntry->getTableID(),
        embeddingProperty->getPropertyID());
    KU_ASSERT_MSG(header != nullptr, "Vector index header not found");
    auto partitionCount = header->getNumPartitions();
    KU_ASSERT_MSG(partitionCount > 0, "Partition count is 0");

    // 0 is the default partition id
    auto mainPartition = header->getPartitionHeader(0);
    KU_ASSERT_MSG(mainPartition != nullptr, "First partition header not found");
    appendScanNodeTable(offset, {tableEntry->getTableID()}, {embedding}, plan, mainPartition->getStartNodeGroupId(),
                        mainPartition->getEndNodeGroupId(), true);
    auto children = logical_op_vector_t();
    children.push_back(plan.getLastOperator());
    auto mainOp = make_shared<LogicalUpdateVectorIndex>(tableEntry->getName(),
                                                    embeddingProperty->getName(), tableEntry->getTableID(),
                                                    embeddingProperty->getPropertyID(),
                                                    offset, embedding, outExprs, 0, children);
    for (auto i = 1; i < partitionCount; i++) {
        auto partition = header->getPartitionHeader(i);
        KU_ASSERT_MSG(partition != nullptr, "Partition header not found");
        appendScanNodeTable(offset, {tableEntry->getTableID()}, {embedding}, plan, partition->getStartNodeGroupId(),
            partition->getEndNodeGroupId(), true);
        auto children = logical_op_vector_t();
        children.push_back(plan.getLastOperator());
        auto op = make_shared<LogicalUpdateVectorIndex>(tableEntry->getName(),
                                                        embeddingProperty->getName(), tableEntry->getTableID(),
                                                        embeddingProperty->getPropertyID(),
                                                        offset, embedding, outExprs, i, children);
        mainOp->setChild(i, std::move(op));
    }
    plan.setLastOperator(std::move(mainOp));
}

} // namespace planner
} // namespace kuzu

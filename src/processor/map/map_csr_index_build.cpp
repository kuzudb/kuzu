#include "binder/expression/property_expression.h"
#include "planner/operator/logical_csr_build.h"
#include "processor/operator/csr_index_build.h"
#include "processor/operator/scan/scan_rel_table.h"
#include "processor/plan_mapper.h"
#include "storage/storage_manager.h"

using namespace kuzu::planner;
using namespace kuzu::storage;
using namespace kuzu::binder;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapCSRIndexBuild(
    planner::LogicalOperator* logicalOperator) {
    auto logicalCSRBuild = (LogicalCSRBuild*)logicalOperator;
    auto child = mapOperator(logicalOperator->getChild(0).get());
    auto schema = logicalOperator->getSchema();
    auto resultSetDescriptor = std::make_unique<ResultSetDescriptor>(schema);
    auto srcNodeExpression =
        ku_dynamic_cast<Expression*, NodeExpression*>(logicalCSRBuild->getBoundNode().get());
    auto boundNodeVectorPos =
        DataPos(schema->getExpressionPos(*srcNodeExpression->getInternalID()));
    auto sharedState = std::make_shared<graph::CSRIndexSharedState>();
    auto relTableID = logicalCSRBuild->getSingleRelTableID();
    auto relTable = clientContext->getStorageManager()->getTable(relTableID)->ptrCast<RelTable>();
    auto printInfo = std::make_unique<OPPrintInfo>("");
    return std::make_unique<CSRIndexBuild>(std::move(resultSetDescriptor), relTable,
        boundNodeVectorPos, sharedState, std::move(child), getOperatorID(), std::move(printInfo));
}

} // namespace processor
} // namespace kuzu

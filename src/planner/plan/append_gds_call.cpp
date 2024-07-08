#include "binder/query/reading_clause/bound_gds_call.h"
#include "planner/operator/logical_csr_build.h"
#include "planner/operator/logical_gds_call.h"
#include "planner/operator/scan/logical_scan_node_table.h"
#include "planner/planner.h"

using namespace kuzu::binder;

namespace kuzu {
namespace planner {

/*
 * The plan for in mem csr build (for now):
 *
 *  GDS Function Call
 *        |
 *  CSR Index Build
 *        |
 *  Scan Node Table
 *
 *  Previously, there was a scan rel table where scanning of rel offsets would be done.
 *  But now since the schema does not have all the recursive rel info like nbr node expression,
 *  rel expression, skipped adding of that operator. The logic of reading nbr offsets just inside
 *  the csr index build operator.
 */
void Planner::appendGDSCall(const binder::BoundReadingClause& readingClause, LogicalPlan& plan) {
    auto& call = readingClause.constCast<BoundGDSCall>();
    auto& gdsInfo = call.getInfo();
    if (gdsInfo.graphEntry.inMemProjection) {
        // scan node id
        expression_vector properties;
        auto srcNodeExpr =
            ku_dynamic_cast<Expression*, NodeExpression*>(gdsInfo.srcNodeIDExpression.get());
        appendScanNodeTable(srcNodeExpr->getInternalID(), gdsInfo.graphEntry.nodeTableIDs,
            properties, plan);
        auto logicalCSRBuild = std::make_shared<LogicalCSRBuild>(gdsInfo.srcNodeIDExpression,
            gdsInfo.graphEntry.relTableIDs, plan.getLastOperator());
        logicalCSRBuild->computeFactorizedSchema();
        auto logicalGDS = std::make_shared<LogicalGDSCall>(call.getInfo().copy(), logicalCSRBuild);
        logicalGDS->computeFactorizedSchema();
        plan.setLastOperator(std::move(logicalGDS));
    } else {
        plan.setLastOperator(std::make_shared<LogicalGDSCall>(call.getInfo().copy()));
    }
}

} // namespace planner
} // namespace kuzu

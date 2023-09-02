#include "binder/copy/bound_copy_from.h"
#include "binder/copy/bound_copy_to.h"
#include "planner/logical_plan/copy/logical_copy_from.h"
#include "planner/logical_plan/copy/logical_copy_to.h"
#include "planner/planner.h"

using namespace kuzu::storage;
using namespace kuzu::catalog;
using namespace kuzu::common;

namespace kuzu {
namespace planner {

std::unique_ptr<LogicalPlan> Planner::planCopyFrom(const BoundStatement& statement) {
    auto& copyClause = reinterpret_cast<const BoundCopyFrom&>(statement);
    auto plan = std::make_unique<LogicalPlan>();
    auto copy = make_shared<LogicalCopyFrom>(copyClause.getCopyDescription()->copy(),
        copyClause.getTableSchema(), copyClause.getColumnExpressions(),
        copyClause.getNodeOffsetExpression(),
        copyClause.getStatementResult()->getSingleExpressionToCollect(),
        copyClause.preservingOrder());
    plan->setLastOperator(std::move(copy));
    return plan;
}

std::unique_ptr<LogicalPlan> Planner::planCopyTo(const Catalog& catalog,
    const NodesStatisticsAndDeletedIDs& nodesStatistics, const RelsStatistics& relsStatistics,
    const BoundStatement& statement) {
    auto& copyClause = reinterpret_cast<const BoundCopyTo&>(statement);
    auto regularQuery = copyClause.getRegularQuery();
    assert(regularQuery->getStatementType() == StatementType::QUERY);
    auto plan = QueryPlanner(catalog, nodesStatistics, relsStatistics).getBestPlan(*regularQuery);
    auto logicalCopyTo = make_shared<LogicalCopyTo>(
        plan->getLastOperator(), copyClause.getCopyDescription()->copy());
    plan->setLastOperator(std::move(logicalCopyTo));
    return plan;
}

} // namespace planner
} // namespace kuzu

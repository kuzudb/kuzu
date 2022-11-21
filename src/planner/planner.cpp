#include "planner/planner.h"

#include "binder/bind/bound_copy_csv.h"
#include "binder/bind/bound_create_node_clause.h"
#include "binder/bind/bound_create_rel_clause.h"
#include "binder/bind/bound_drop_table.h"
#include "planner/logical_plan/logical_operator/logical_copy_csv.h"
#include "planner/logical_plan/logical_operator/logical_create_node_table.h"
#include "planner/logical_plan/logical_operator/logical_create_rel_table.h"
#include "planner/logical_plan/logical_operator/logical_drop_table.h"

namespace kuzu {
namespace planner {

unique_ptr<LogicalPlan> Planner::getBestPlan(const Catalog& catalog,
    const NodesStatisticsAndDeletedIDs& nodesStatistics, const RelsStatistics& relsStatistics,
    const BoundStatement& statement) {
    switch (statement.getStatementType()) {
    case StatementType::QUERY: {
        return QueryPlanner(catalog, nodesStatistics, relsStatistics).getBestPlan(statement);
    }
    case StatementType::CREATE_NODE_CLAUSE: {
        return planCreateNodeTable(statement);
    }
    case StatementType::CREATE_REL_CLAUSE: {
        return planCreateRelTable(statement);
    }
    case StatementType::DROP_TABLE: {
        return planDropTable(statement);
    }
    case StatementType::COPY_CSV: {
        return planCopyCSV(statement);
    }
    default:
        assert(false);
    }
}

vector<unique_ptr<LogicalPlan>> Planner::getAllPlans(const Catalog& catalog,
    const NodesStatisticsAndDeletedIDs& nodesStatistics, const RelsStatistics& relsStatistics,
    const BoundStatement& statement) {
    // We enumerate all plans for our testing framework. This API should only be used for QUERY but
    // not DDL or COPY_CSV.
    assert(statement.getStatementType() == StatementType::QUERY);
    return QueryPlanner(catalog, nodesStatistics, relsStatistics).getAllPlans(statement);
}

unique_ptr<LogicalPlan> Planner::planCreateNodeTable(const BoundStatement& statement) {
    auto& createNodeClause = (BoundCreateNodeClause&)statement;
    auto plan = make_unique<LogicalPlan>();
    plan->setLastOperator(make_shared<LogicalCreateNodeTable>(createNodeClause.getTableName(),
        createNodeClause.getPropertyNameDataTypes(), createNodeClause.getPrimaryKeyIdx()));
    return plan;
}

unique_ptr<LogicalPlan> Planner::planCreateRelTable(const BoundStatement& statement) {
    auto& createRelClause = (BoundCreateRelClause&)statement;
    auto plan = make_unique<LogicalPlan>();
    plan->setLastOperator(make_shared<LogicalCreateRelTable>(createRelClause.getTableName(),
        createRelClause.getPropertyNameDataTypes(), createRelClause.getRelMultiplicity(),
        createRelClause.getSrcDstTableIDs()));
    return plan;
}

unique_ptr<LogicalPlan> Planner::planDropTable(const BoundStatement& statement) {
    auto& dropTable = (BoundDropTable&)statement;
    auto plan = make_unique<LogicalPlan>();
    plan->setLastOperator(make_shared<LogicalDropTable>(dropTable.getTableSchema()));
    return plan;
}

unique_ptr<LogicalPlan> Planner::planCopyCSV(const BoundStatement& statement) {
    auto& copyCSV = (BoundCopyCSV&)statement;
    auto plan = make_unique<LogicalPlan>();
    plan->setLastOperator(
        make_shared<LogicalCopyCSV>(copyCSV.getCSVDescription(), copyCSV.getTableSchema()));
    return plan;
}

} // namespace planner
} // namespace kuzu

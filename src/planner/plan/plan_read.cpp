#include "binder/query/reading_clause/bound_match_clause.h"
#include "planner/query_planner.h"

using namespace kuzu::common;

namespace kuzu {
namespace planner {

void QueryPlanner::planReadingClause(
    BoundReadingClause* boundReadingClause, std::vector<std::unique_ptr<LogicalPlan>>& prevPlans) {
    auto readingClauseType = boundReadingClause->getClauseType();
    switch (readingClauseType) {
    case ClauseType::MATCH: {
        planMatchClause(boundReadingClause, prevPlans);
    } break;
    case ClauseType::UNWIND: {
        planUnwindClause(boundReadingClause, prevPlans);
    } break;
    case ClauseType::IN_QUERY_CALL: {
        planInQueryCall(boundReadingClause, prevPlans);
    } break;
    default:
        // LCOV_EXCL_START
        throw NotImplementedException("QueryPlanner::planReadingClause");
        // LCOV_EXCL_STOP
    }
}

void QueryPlanner::planMatchClause(
    BoundReadingClause* boundReadingClause, std::vector<std::unique_ptr<LogicalPlan>>& plans) {
    auto boundMatchClause = reinterpret_cast<BoundMatchClause*>(boundReadingClause);
    auto queryGraphCollection = boundMatchClause->getQueryGraphCollection();
    auto predicates = boundMatchClause->getPredicatesSplitOnAnd();
    switch (boundMatchClause->getMatchClauseType()) {
    case MatchClauseType::MATCH: {
        if (plans.size() == 1 && plans[0]->isEmpty()) {
            plans = enumerateQueryGraphCollection(*queryGraphCollection, predicates);
        } else {
            for (auto& plan : plans) {
                planRegularMatch(*queryGraphCollection, predicates, *plan);
            }
        }
    } break;
    case MatchClauseType::OPTIONAL_MATCH: {
        for (auto& plan : plans) {
            planOptionalMatch(*queryGraphCollection, predicates, *plan);
        }
    } break;
    default:
        throw NotImplementedException("QueryPlanner::planMatchClause");
    }
}

void QueryPlanner::planUnwindClause(
    BoundReadingClause* boundReadingClause, std::vector<std::unique_ptr<LogicalPlan>>& plans) {
    for (auto& plan : plans) {
        if (plan->isEmpty()) { // UNWIND [1, 2, 3, 4] AS x RETURN x
            appendDummyScan(*plan);
        }
        appendUnwind(*boundReadingClause, *plan);
    }
}

void QueryPlanner::planInQueryCall(
    BoundReadingClause* boundReadingClause, std::vector<std::unique_ptr<LogicalPlan>>& plans) {
    for (auto& plan : plans) {
        if (!plan->isEmpty()) {
            auto tmpPlan = std::make_unique<LogicalPlan>();
            appendInQueryCall(*boundReadingClause, *tmpPlan);
            appendCrossProduct(AccumulateType::REGULAR, *plan, *tmpPlan);
        } else {
            appendInQueryCall(*boundReadingClause, *plan);
        }
    }
}

} // namespace planner
} // namespace kuzu

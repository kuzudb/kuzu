#include "binder/query/reading_clause/bound_in_query_call.h"
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
    case ClauseType::InQueryCall: {
        planInQueryCall(boundReadingClause, prevPlans);
    } break;
    default:
        throw NotImplementedException("QueryPlanner::planReadingClause");
    }
}

void QueryPlanner::planMatchClause(
    BoundReadingClause* boundReadingClause, std::vector<std::unique_ptr<LogicalPlan>>& plans) {
    auto boundMatchClause = reinterpret_cast<BoundMatchClause*>(boundReadingClause);
    auto queryGraphCollection = boundMatchClause->getQueryGraphCollection();
    auto predicates = boundMatchClause->getPredicatesSplitOnAnd();
    switch (boundMatchClause->getMatchClauseType()) {
    case common::MatchClauseType::MATCH: {
        if (plans.size() == 1 && plans[0]->isEmpty()) {
            plans = enumerateQueryGraphCollection(*queryGraphCollection, predicates);
        } else {
            for (auto& plan : plans) {
                planRegularMatch(*queryGraphCollection, predicates, *plan);
            }
        }
    } break;
    case common::MatchClauseType::OPTIONAL_MATCH: {
        for (auto& plan : plans) {
            planOptionalMatch(*queryGraphCollection, predicates, *plan);
        }
    } break;
    default:
        throw common::NotImplementedException("QueryPlanner::planMatchClause");
    }
}

void QueryPlanner::planUnwindClause(
    BoundReadingClause* boundReadingClause, std::vector<std::unique_ptr<LogicalPlan>>& plans) {
    auto boundUnwindClause = reinterpret_cast<BoundUnwindClause*>(boundReadingClause);
    for (auto& plan : plans) {
        if (plan->isEmpty()) { // UNWIND [1, 2, 3, 4] AS x RETURN x
            appendDummyScan(*plan);
        }
        appendUnwind(*boundUnwindClause, *plan);
    }
}

void QueryPlanner::planInQueryCall(
    BoundReadingClause* boundReadingClause, std::vector<std::unique_ptr<LogicalPlan>>& plans) {
    auto boundInQueryCall = reinterpret_cast<BoundInQueryCall*>(boundReadingClause);
    for (auto& plan : plans) {
        if (!plan->isEmpty()) {
            auto inQueryCallPlan = std::make_shared<LogicalPlan>();
            appendInQueryCall(*boundInQueryCall, *inQueryCallPlan);
            appendCrossProduct(common::AccumulateType::REGULAR, *plan, *inQueryCallPlan);
        } else {
            appendInQueryCall(*boundInQueryCall, *plan);
        }
    }
}

} // namespace planner
} // namespace kuzu

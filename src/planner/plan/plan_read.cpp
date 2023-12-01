#include "binder/expression_visitor.h"
#include "binder/query/reading_clause/bound_in_query_call.h"
#include "binder/query/reading_clause/bound_load_from.h"
#include "binder/query/reading_clause/bound_match_clause.h"
#include "planner/query_planner.h"

using namespace kuzu::binder;
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
    case ClauseType::LOAD_FROM: {
        planLoadFrom(boundReadingClause, prevPlans);
    } break;
    default:
        KU_UNREACHABLE;
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
        KU_UNREACHABLE;
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

static bool hasExternalDependency(const std::shared_ptr<Expression>& expression,
    const std::unordered_set<std::string>& variableNameSet) {
    auto collector = ExpressionCollector();
    for (auto& name : collector.getDependentVariableNames(expression)) {
        if (!variableNameSet.contains(name)) {
            return true;
        }
    }
    return false;
}

void QueryPlanner::planInQueryCall(
    BoundReadingClause* readingClause, std::vector<std::unique_ptr<LogicalPlan>>& plans) {
    auto inQueryCall = reinterpret_cast<BoundInQueryCall*>(readingClause);
    std::unordered_set<std::string> columnNameSet;
    for (auto& column : inQueryCall->getOutputExpressions()) {
        columnNameSet.insert(column->getUniqueName());
    }
    expression_vector predicatesToPushDown;
    expression_vector predicatesToPullUp;
    for (auto& predicate : inQueryCall->getPredicatesSplitOnAnd()) {
        if (hasExternalDependency(predicate, columnNameSet)) {
            predicatesToPullUp.push_back(predicate);
        } else {
            predicatesToPushDown.push_back(predicate);
        }
    }
    for (auto& plan : plans) {
        if (!plan->isEmpty()) {
            auto tmpPlan = std::make_unique<LogicalPlan>();
            appendInQueryCall(*readingClause, *tmpPlan);
            if (!predicatesToPushDown.empty()) {
                appendFilters(predicatesToPushDown, *tmpPlan);
            }
            appendCrossProduct(AccumulateType::REGULAR, *plan, *tmpPlan);
        } else {
            appendInQueryCall(*readingClause, *plan);
            if (!predicatesToPushDown.empty()) {
                appendFilters(predicatesToPushDown, *plan);
            }
        }
        if (!predicatesToPullUp.empty()) {
            appendFilter(inQueryCall->getWherePredicate(), *plan);
        }
    }
}

void QueryPlanner::planLoadFrom(
    binder::BoundReadingClause* readingClause, std::vector<std::unique_ptr<LogicalPlan>>& plans) {
    auto loadFrom = reinterpret_cast<BoundLoadFrom*>(readingClause);
    std::unordered_set<std::string> columnNameSet;
    for (auto& column : loadFrom->getInfo()->columns) {
        columnNameSet.insert(column->getUniqueName());
    }
    expression_vector predicatesToPushDown;
    expression_vector predicatesToPullUp;
    for (auto& predicate : loadFrom->getPredicatesSplitOnAnd()) {
        if (hasExternalDependency(predicate, columnNameSet)) {
            predicatesToPullUp.push_back(predicate);
        } else {
            predicatesToPushDown.push_back(predicate);
        }
    }
    for (auto& plan : plans) {
        if (!plan->isEmpty()) {
            auto tmpPlan = std::make_unique<LogicalPlan>();
            appendScanFile(loadFrom->getInfo(), *tmpPlan);
            if (!predicatesToPushDown.empty()) {
                appendFilters(predicatesToPushDown, *tmpPlan);
            }
            appendCrossProduct(AccumulateType::REGULAR, *plan, *tmpPlan);
        } else {
            appendScanFile(loadFrom->getInfo(), *plan);
            if (!predicatesToPushDown.empty()) {
                appendFilters(predicatesToPushDown, *plan);
            }
        }
        if (!predicatesToPullUp.empty()) {
            appendFilter(loadFrom->getWherePredicate(), *plan);
        }
    }
}

} // namespace planner
} // namespace kuzu

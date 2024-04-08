#include "binder/expression_visitor.h"
#include "binder/query/reading_clause/bound_in_query_call.h"
#include "binder/query/reading_clause/bound_load_from.h"
#include "binder/query/reading_clause/bound_match_clause.h"
#include "common/enums/join_type.h"
#include "planner/planner.h"

using namespace kuzu::binder;
using namespace kuzu::common;

namespace kuzu {
namespace planner {

void Planner::planReadingClause(const BoundReadingClause* readingClause,
    std::vector<std::unique_ptr<LogicalPlan>>& prevPlans) {
    auto readingClauseType = readingClause->getClauseType();
    switch (readingClauseType) {
    case ClauseType::MATCH: {
        planMatchClause(readingClause, prevPlans);
    } break;
    case ClauseType::UNWIND: {
        planUnwindClause(readingClause, prevPlans);
    } break;
    case ClauseType::IN_QUERY_CALL: {
        planInQueryCall(readingClause, prevPlans);
    } break;
    case ClauseType::LOAD_FROM: {
        planLoadFrom(readingClause, prevPlans);
    } break;
    default:
        KU_UNREACHABLE;
    }
}

void Planner::planMatchClause(const BoundReadingClause* boundReadingClause,
    std::vector<std::unique_ptr<LogicalPlan>>& plans) {
    auto boundMatchClause =
        ku_dynamic_cast<const BoundReadingClause*, const BoundMatchClause*>(boundReadingClause);
    auto queryGraphCollection = boundMatchClause->getQueryGraphCollection();
    auto predicates = boundMatchClause->getConjunctivePredicates();
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
            expression_vector corrExprs;
            if (!plan->isEmpty()) {
                corrExprs =
                    getCorrelatedExprs(*queryGraphCollection, predicates, plan->getSchema());
            }
            planOptionalMatch(*queryGraphCollection, predicates, corrExprs, *plan);
        }
    } break;
    default:
        KU_UNREACHABLE;
    }
}

void Planner::planUnwindClause(const BoundReadingClause* boundReadingClause,
    std::vector<std::unique_ptr<LogicalPlan>>& plans) {
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

void Planner::planInQueryCall(const BoundReadingClause* readingClause,
    std::vector<std::unique_ptr<LogicalPlan>>& plans) {
    auto inQueryCall =
        ku_dynamic_cast<const BoundReadingClause*, const BoundInQueryCall*>(readingClause);
    std::unordered_set<std::string> columnNameSet;
    for (auto& column : inQueryCall->getOutExprs()) {
        columnNameSet.insert(column->getUniqueName());
    }
    expression_vector predicatesToPushDown;
    expression_vector predicatesToPullUp;
    for (auto& predicate : inQueryCall->getConjunctivePredicates()) {
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
            appendCrossProduct(AccumulateType::REGULAR, *plan, *tmpPlan, *plan);
        } else {
            appendInQueryCall(*readingClause, *plan);
            if (!predicatesToPushDown.empty()) {
                appendFilters(predicatesToPushDown, *plan);
            }
        }
        if (!predicatesToPullUp.empty()) {
            appendFilter(inQueryCall->getPredicate(), *plan);
        }
    }
}

void Planner::planLoadFrom(const BoundReadingClause* readingClause,
    std::vector<std::unique_ptr<LogicalPlan>>& plans) {
    auto loadFrom = ku_dynamic_cast<const BoundReadingClause*, const BoundLoadFrom*>(readingClause);
    std::unordered_set<std::string> columnNameSet;
    for (auto& column : loadFrom->getInfo()->columns) {
        columnNameSet.insert(column->getUniqueName());
    }
    expression_vector predicatesToPushDown;
    expression_vector predicatesToPullUp;
    for (auto& predicate : loadFrom->getConjunctivePredicates()) {
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
            appendCrossProduct(AccumulateType::REGULAR, *plan, *tmpPlan, *plan);
        } else {
            appendScanFile(loadFrom->getInfo(), *plan);
            if (!predicatesToPushDown.empty()) {
                appendFilters(predicatesToPushDown, *plan);
            }
        }
        if (!predicatesToPullUp.empty()) {
            appendFilter(loadFrom->getPredicate(), *plan);
        }
    }
}

} // namespace planner
} // namespace kuzu

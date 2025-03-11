#include "binder/expression_visitor.h"
#include "binder/query/reading_clause/bound_load_from.h"
#include "binder/query/reading_clause/bound_match_clause.h"
#include "planner/operator/logical_table_function_call.h"
#include "planner/planner.h"

using namespace kuzu::binder;
using namespace kuzu::common;

namespace kuzu {
namespace planner {

void Planner::planReadingClause(const BoundReadingClause& readingClause,
    std::vector<std::unique_ptr<LogicalPlan>>& prevPlans) {
    switch (readingClause.getClauseType()) {
    case ClauseType::MATCH: {
        planMatchClause(readingClause, prevPlans);
    } break;
    case ClauseType::UNWIND: {
        planUnwindClause(readingClause, prevPlans);
    } break;
    case ClauseType::TABLE_FUNCTION_CALL: {
        planTableFunctionCall(readingClause, prevPlans);
    } break;
    case ClauseType::LOAD_FROM: {
        planLoadFrom(readingClause, prevPlans);
    } break;
    default:
        KU_UNREACHABLE;
    }
}

void Planner::planMatchClause(const BoundReadingClause& readingClause,
    std::vector<std::unique_ptr<LogicalPlan>>& plans) {
    auto& boundMatchClause = readingClause.constCast<BoundMatchClause>();
    auto queryGraphCollection = boundMatchClause.getQueryGraphCollection();
    auto predicates = boundMatchClause.getConjunctivePredicates();
    switch (boundMatchClause.getMatchClauseType()) {
    case MatchClauseType::MATCH: {
        if (plans.size() == 1 && plans[0]->isEmpty()) {
            auto info = QueryGraphPlanningInfo();
            info.predicates = predicates;
            info.hint = boundMatchClause.getHint();
            plans = enumerateQueryGraphCollection(*queryGraphCollection, info);
        } else {
            for (auto& plan : plans) {
                planRegularMatch(*queryGraphCollection, predicates, *plan,
                    boundMatchClause.getHint());
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
            planOptionalMatch(*queryGraphCollection, predicates, corrExprs, *plan,
                boundMatchClause.getHint());
        }
    } break;
    default:
        KU_UNREACHABLE;
    }
}

void Planner::planUnwindClause(const BoundReadingClause& boundReadingClause,
    std::vector<std::unique_ptr<LogicalPlan>>& plans) {
    for (auto& plan : plans) {
        if (plan->isEmpty()) { // UNWIND [1, 2, 3, 4] AS x RETURN x
            appendDummyScan(*plan);
        }
        appendUnwind(boundReadingClause, *plan);
    }
}

static bool hasExternalDependency(const std::shared_ptr<Expression>& expression,
    const std::unordered_set<std::string>& variableNameSet) {
    auto collector = DependentVarNameCollector();
    collector.visit(expression);
    for (auto& name : collector.getVarNames()) {
        if (!variableNameSet.contains(name)) {
            return true;
        }
    }
    return false;
}

void Planner::splitPredicates(const expression_vector& outputExprs,
    const expression_vector& predicates, expression_vector& predicatesToPull,
    expression_vector& predicatesToPush) {
    std::unordered_set<std::string> columnNameSet;
    for (auto& column : outputExprs) {
        columnNameSet.insert(column->getUniqueName());
    }
    for (auto& predicate : predicates) {
        if (hasExternalDependency(predicate, columnNameSet)) {
            predicatesToPull.push_back(predicate);
        } else {
            predicatesToPush.push_back(predicate);
        }
    }
}

void Planner::planTableFunctionCall(const BoundReadingClause& readingClause,
    std::vector<std::unique_ptr<LogicalPlan>>& plans) {
    const auto op = getTableFunctionCall(readingClause);
    const auto planFunc =
        op->ptrCast<LogicalTableFunctionCall>()->getTableFunc().getLogicalPlanFunc;
    KU_ASSERT(planFunc);
    planFunc(this, readingClause, op, plans);
}

void Planner::planLoadFrom(const BoundReadingClause& readingClause,
    std::vector<std::unique_ptr<LogicalPlan>>& plans) {
    auto& loadFrom = readingClause.constCast<BoundLoadFrom>();
    expression_vector predicatesToPull;
    expression_vector predicatesToPush;
    splitPredicates(loadFrom.getInfo()->bindData->columns, loadFrom.getConjunctivePredicates(),
        predicatesToPull, predicatesToPush);
    for (auto& plan : plans) {
        auto op = getTableFunctionCall(*loadFrom.getInfo());
        op->computeFactorizedSchema();
        planReadOp(std::move(op), predicatesToPush, *plan);
        if (!predicatesToPull.empty()) {
            appendFilters(predicatesToPull, *plan);
        }
    }
}

void Planner::planReadOp(std::shared_ptr<LogicalOperator> op, const expression_vector& predicates,
    LogicalPlan& plan) {
    op->computeFactorizedSchema();
    if (!plan.isEmpty()) {
        auto tmpPlan = LogicalPlan();
        tmpPlan.setLastOperator(std::move(op));
        if (!predicates.empty()) {
            appendFilters(predicates, tmpPlan);
        }
        appendCrossProduct(plan, tmpPlan, plan);
    } else {
        plan.setLastOperator(std::move(op));
        if (!predicates.empty()) {
            appendFilters(predicates, plan);
        }
    }
}

} // namespace planner
} // namespace kuzu

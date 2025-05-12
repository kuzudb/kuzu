#include "binder/query/bound_regular_query.h"
#include "planner/operator/logical_union.h"
#include "planner/planner.h"

using namespace kuzu::binder;
using namespace kuzu::common;

namespace kuzu {
namespace planner {

LogicalPlan Planner::planQuery(const BoundStatement& boundStatement) {
    std::unique_ptr<LogicalPlan> resultPlans;
    auto& regularQuery = boundStatement.constCast<BoundRegularQuery>();
    if (regularQuery.getNumSingleQueries() == 1) {
        return planSingleQuery(*regularQuery.getSingleQuery(0));
    }
    std::vector<LogicalPlan> childrenPlans;
    for (auto i = 0u; i < regularQuery.getNumSingleQueries(); i++) {
        childrenPlans.push_back(planSingleQuery(*regularQuery.getSingleQuery(i)));
    }
    return createUnionPlan(childrenPlans, regularQuery.getIsUnionAll(0));
}

LogicalPlan Planner::createUnionPlan(std::vector<LogicalPlan>& childrenPlans, bool isUnionAll) {
    KU_ASSERT(!childrenPlans.empty());
    auto plan = LogicalPlan();
    std::vector<std::shared_ptr<LogicalOperator>> children;
    children.reserve(childrenPlans.size());
    for (auto& childPlan : childrenPlans) {
        children.push_back(childPlan.getLastOperator());
    }
    // we compute the schema based on first child
    auto union_ = std::make_shared<LogicalUnion>(
        childrenPlans[0].getSchema()->getExpressionsInScope(), std::move(children));
    for (auto i = 0u; i < childrenPlans.size(); ++i) {
        appendFlattens(union_->getGroupsPosToFlatten(i), childrenPlans[i]);
        union_->setChild(i, childrenPlans[i].getLastOperator());
    }
    union_->computeFactorizedSchema();
    plan.setLastOperator(union_);
    if (!isUnionAll) {
        appendDistinct(union_->getExpressionsToUnion(), plan);
    }
    return plan;
}

expression_vector Planner::getProperties(const Expression& pattern) const {
    KU_ASSERT(pattern.expressionType == ExpressionType::PATTERN);
    return propertyExprCollection.getProperties(pattern);
}

JoinOrderEnumeratorContext Planner::enterNewContext() {
    auto prevContext = std::move(context);
    context = JoinOrderEnumeratorContext();
    return prevContext;
}

void Planner::exitContext(JoinOrderEnumeratorContext prevContext) {
    context = std::move(prevContext);
}

PropertyExprCollection Planner::enterNewPropertyExprCollection() {
    auto prevCollection = std::move(propertyExprCollection);
    propertyExprCollection = PropertyExprCollection();
    return prevCollection;
}

void Planner::exitPropertyExprCollection(PropertyExprCollection collection) {
    propertyExprCollection = std::move(collection);
}

} // namespace planner
} // namespace kuzu

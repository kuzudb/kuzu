#include "binder/expression/property_expression.h"
#include "binder/visitor/property_collector.h"
#include "planner/planner.h"

using namespace kuzu::binder;

namespace kuzu {
namespace planner {

// Note: we cannot append ResultCollector for plans enumerated for single query before there could
// be a UNION on top which requires further flatten. So we delay ResultCollector appending to
// enumerate regular query level.
std::vector<std::unique_ptr<LogicalPlan>> Planner::planSingleQuery(
    const NormalizedSingleQuery* singleQuery) {
    auto propertyCollector = binder::PropertyCollector();
    propertyCollector.visitSingleQuery(*singleQuery);
    auto properties = propertyCollector.getProperties();
    for (auto& expr : propertyCollector.getProperties()) {
        auto& property = expr->constCast<PropertyExpression>();
        propertyExprCollection.addProperties(property.getVariableName(), expr);
    }
    context.resetState();
    auto plans = getInitialEmptyPlans();
    for (auto i = 0u; i < singleQuery->getNumQueryParts(); ++i) {
        plans = planQueryPart(singleQuery->getQueryPart(i), std::move(plans));
    }
    return plans;
}

std::vector<std::unique_ptr<LogicalPlan>> Planner::planQueryPart(
    const NormalizedQueryPart* queryPart, std::vector<std::unique_ptr<LogicalPlan>> prevPlans) {
    std::vector<std::unique_ptr<LogicalPlan>> plans = std::move(prevPlans);
    // plan read
    for (auto i = 0u; i < queryPart->getNumReadingClause(); i++) {
        planReadingClause(*queryPart->getReadingClause(i), plans);
    }
    // plan update
    for (auto i = 0u; i < queryPart->getNumUpdatingClause(); ++i) {
        planUpdatingClause(queryPart->getUpdatingClause(i), plans);
    }
    if (queryPart->hasProjectionBody()) {
        planProjectionBody(queryPart->getProjectionBody(), plans);
        if (queryPart->hasProjectionBodyPredicate()) {
            for (auto& plan : plans) {
                appendFilter(queryPart->getProjectionBodyPredicate(), *plan);
            }
        }
    }
    return plans;
}

} // namespace planner
} // namespace kuzu

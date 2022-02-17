#pragma once

#include "src/binder/expression/include/property_expression.h"
#include "src/binder/query/include/normalized_single_query.h"

using namespace graphflow::binder;

namespace graphflow {
namespace planner {

using property_vector = vector<shared_ptr<PropertyExpression>>;

class EnumeratorUtils {

public:
    static property_vector getPropertiesToScan(const NormalizedSingleQuery& singleQuery);

    // find all property expressions under the subtree rooted at expression
    static property_vector getProperties(const shared_ptr<Expression>& expression);

    static property_vector getPropertiesForNode(
        const property_vector& propertiesToScan, const NodeExpression& node, bool isStructured);

    static property_vector getPropertiesForRel(
        const property_vector& propertiesToScan, const RelExpression& rel);

private:
    static property_vector getPropertiesToScan(const BoundProjectionBody& projectionBody);
};

} // namespace planner
} // namespace graphflow

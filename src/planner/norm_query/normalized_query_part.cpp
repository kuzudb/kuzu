#include "src/planner/include/norm_query/normalized_query_part.h"

#include "src/binder/include/expression/property_expression.h"

namespace graphflow {
namespace planner {

vector<shared_ptr<Expression>> NormalizedQueryPart::getDependentNodeID() const {
    vector<shared_ptr<Expression>> result;
    for (auto& node : queryGraph->queryNodes) {
        result.push_back(node->getNodeIDPropertyExpression());
    }
    return result;
}

} // namespace planner
} // namespace graphflow

#include "planner/join_order_enumerator_context.h"

#include <memory>

#include "binder/expression/expression.h"
#include "binder/query/query_graph.h"
#include "planner/subplans_table.h"

using namespace kuzu::binder;

namespace kuzu {
namespace planner {

void JoinOrderEnumeratorContext::init(
    QueryGraph* queryGraph_, const expression_vector& predicates) {
    whereExpressionsSplitOnAND = predicates;
    this->queryGraph = queryGraph_;
    // clear and resize subPlansTable
    subPlansTable->clear();
    maxLevel = queryGraph_->getNumQueryNodes() + queryGraph_->getNumQueryRels() + 1;
    subPlansTable->resize(maxLevel);
    // Restart from level 1 for new query part so that we get hashJoin based plans
    // that uses subplans coming from previous query part.See example in planRelIndexJoin().
    currentLevel = 1;
}

SubqueryGraph JoinOrderEnumeratorContext::getFullyMatchedSubqueryGraph() const {
    auto subqueryGraph = SubqueryGraph(*queryGraph);
    for (auto i = 0u; i < queryGraph->getNumQueryNodes(); ++i) {
        subqueryGraph.addQueryNode(i);
    }
    for (auto i = 0u; i < queryGraph->getNumQueryRels(); ++i) {
        subqueryGraph.addQueryRel(i);
    }
    return subqueryGraph;
}

void JoinOrderEnumeratorContext::resetState() {
    subPlansTable = std::make_unique<SubPlansTable>();
}

} // namespace planner
} // namespace kuzu

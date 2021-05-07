#include "src/planner/include/query_planner.h"

#include "src/parser/include/parser.h"
#include "src/planner/include/binder.h"
#include "src/planner/include/enumerator.h"

using namespace graphflow::parser;

namespace graphflow {
namespace planner {

vector<unique_ptr<LogicalPlan>> QueryPlanner::enumeratePlans(string query, const Catalog& catalog) {
    Parser parser;
    auto singleQuery = parser.parseQuery(query);
    Binder binder(catalog);
    auto boundSingleQuery = binder.bindSingleQuery(*singleQuery);
    Enumerator enumerator(catalog, *boundSingleQuery);
    return enumerator.enumeratePlans();
}

} // namespace planner
} // namespace graphflow

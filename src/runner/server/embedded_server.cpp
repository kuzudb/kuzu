#include "src/runner/include/server/embedded_server.h"

#include "src/parser/include/parser.h"
#include "src/planner/include/binder.h"
#include "src/planner/include/enumerator.h"
#include "src/processor/include/physical_plan/plan_mapper.h"

using namespace graphflow::processor;
using namespace graphflow::planner;

namespace graphflow {
namespace runner {

vector<unique_ptr<LogicalPlan>> EmbeddedServer::enumerateLogicalPlans(const string& query) {
    graphflow::parser::Parser parser;
    auto singleQuery = parser.parseQuery(query);
    Binder binder(graph->getCatalog());
    auto boundStatements = binder.bindSingleQuery(*singleQuery);
    Enumerator enumerator(boundStatements[0]->getQueryGraph());
    return enumerator.enumeratePlans();
}

unique_ptr<QueryResult> EmbeddedServer::execute(unique_ptr<LogicalPlan> plan) {
    auto physicalPlan = PlanMapper::mapToPhysical(move(plan), *graph);
    auto result = processor->execute(move(physicalPlan), parallelism);
    return result;
}
} // namespace runner
} // namespace graphflow

#pragma once

#include <cstdint>

#include "src/loader/include/graph_loader.h"
#include "src/planner/include/logical_plan/logical_plan.h"
#include "src/processor/include/physical_plan/query_result.h"
#include "src/processor/include/processor.h"
#include "src/storage/include/graph.h"

using namespace graphflow::processor;
using namespace graphflow::storage;
using namespace graphflow::loader;
using namespace graphflow::planner;

namespace graphflow {
namespace runner {

class EmbeddedServer {

public:
    EmbeddedServer(const string& inputGraphPath, const string& outputGraphPath,
        uint64_t parallelism, uint64_t bufferPoolSize = DEFAULT_BUFFER_POOL_SIZE)
        : parallelism(parallelism) {
        GraphLoader graphLoader(inputGraphPath, outputGraphPath, parallelism);
        graphLoader.loadGraph();
        graph = make_unique<Graph>(outputGraphPath, bufferPoolSize);
        processor = make_unique<QueryProcessor>(parallelism);
    }

    vector<unique_ptr<LogicalPlan>> enumerateLogicalPlans(const string& query);
    unique_ptr<QueryResult> execute(unique_ptr<LogicalPlan> plan);

private:
    uint64_t parallelism;
    unique_ptr<Graph> graph;
    unique_ptr<QueryProcessor> processor;
};

} // namespace runner
} // namespace graphflow

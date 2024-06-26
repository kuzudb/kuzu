#pragma once

#include <atomic>
#include <condition_variable>
#include <mutex>

#include "common/task_system/task_scheduler.h"
#include "function/gds/new_frontier.h"
#include "function/table_functions.h"

using namespace kuzu::processor;

namespace kuzu {
namespace function {

// TODO(Semih): Rename to GDSUtils
class GDSUtils {
public:
    explicit GDSUtils();
    
    static void parallelizeFrontierVertexUpdate(ExecutionContext* executionContext,
        Frontiers& frontiers, graph::Graph* graph, FrontierUpdateFn& vu);
    static void runFrontiersUntilConvergence(ExecutionContext* executionContext,
        Frontiers& frontiers, graph::Graph* graph, FrontierUpdateFn& vu, uint64_t maxIters);
};

} // namespace function
} // namespace kuzu
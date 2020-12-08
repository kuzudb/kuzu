#pragma once

#include <vector>

#include "src/processor/include/operator/operator.h"
#include "src/processor/include/task_system/morsel.h"

using namespace std;

namespace graphflow {
namespace processor {

// A single linear plan.
class Subplan {
    unique_ptr<Operator> lastOperator;

public:
    void initialize(Graph* graph, shared_ptr<MorselDesc>& morsel) {
        lastOperator->initialize(graph, morsel);
    }

    void run() {
        while (lastOperator->getNextMorsel()) {
            lastOperator->getNextTuples();
        }
    }
};

class QueryPlan {
    vector<Subplan> subplans;
};

} // namespace processor
} // namespace graphflow

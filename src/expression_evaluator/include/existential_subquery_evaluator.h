#pragma once

#include "src/expression_evaluator/include/expression_evaluator.h"
#include "src/processor/include/physical_plan/operator/scan_node_id/select_scan.h"
#include "src/processor/include/physical_plan/operator/sink/result_collector.h"

using namespace graphflow::processor;

namespace graphflow {
namespace evaluator {

class ExistentialSubqueryEvaluator : public ExpressionEvaluator {

public:
    ExistentialSubqueryEvaluator(
        MemoryManager& memoryManager, unique_ptr<ResultCollector> subPlanResultCollector);

    void executeSubplan();

    void evaluate() override;
    uint64_t select(sel_t* selectedPositions) override;

private:
    unique_ptr<ResultCollector> subPlanResultCollector;
};

} // namespace evaluator
} // namespace graphflow

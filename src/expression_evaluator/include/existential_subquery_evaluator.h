#pragma once

#include "src/expression_evaluator/include/expression_evaluator.h"
#include "src/processor/include/physical_plan/operator/sink/result_collector.h"

using namespace graphflow::processor;

namespace graphflow {
namespace evaluator {

class ExistentialSubqueryEvaluator : public ExpressionEvaluator {

public:
    ExistentialSubqueryEvaluator(
        MemoryManager& memoryManager, unique_ptr<ResultCollector> subPlanResultCollector);

    uint64_t executeSubplan();

    void evaluate() override;
    uint64_t select(sel_t* selectedPositions) override;

    unique_ptr<ExpressionEvaluator> clone(
        MemoryManager& memoryManager, const ResultSet& resultSet) override;

private:
    unique_ptr<ResultCollector> subPlanResultCollector;
};

} // namespace evaluator
} // namespace graphflow

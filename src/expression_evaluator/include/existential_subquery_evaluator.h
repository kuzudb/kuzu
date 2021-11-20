#pragma once

#include "src/expression_evaluator/include/expression_evaluator.h"
#include "src/processor/include/physical_plan/operator/result_collector.h"

using namespace graphflow::processor;

namespace graphflow {
namespace evaluator {

class ExistentialSubqueryEvaluator : public ExpressionEvaluator {

public:
    explicit ExistentialSubqueryEvaluator(unique_ptr<ResultCollector> subPlanResultCollector);

    void initResultSet(const ResultSet& resultSet, MemoryManager& memoryManager) override;

    uint64_t executeSubplan();

    void evaluate() override;
    uint64_t select(sel_t* selectedPositions) override;

    unique_ptr<ExpressionEvaluator> clone() override;

private:
    unique_ptr<ResultCollector> subPlanResultCollector;
};

} // namespace evaluator
} // namespace graphflow

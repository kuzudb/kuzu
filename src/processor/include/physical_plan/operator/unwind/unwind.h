#pragma once

#include "src/binder/expression/include/expression.h"
#include "src/common/include/vector/value_vector_utils.h"
#include "src/expression_evaluator/include/base_evaluator.h"
#include "src/processor/include/physical_plan/operator/physical_operator.h"
#include "src/processor/include/physical_plan/operator/source_operator.h"
#include "src/processor/include/physical_plan/result/result_set.h"

using namespace graphflow::evaluator;

namespace graphflow {
namespace processor {

class Unwind : public PhysicalOperator, public SourceOperator {
public:
    Unwind(uint32_t id, const string& paramsString, shared_ptr<Expression> expression,
        unique_ptr<ResultSetDescriptor> resultSetDescriptor, DataPos outDataPos,
        unique_ptr<BaseExpressionEvaluator> expressionEvaluator)
        : PhysicalOperator{id, paramsString}, SourceOperator{move(resultSetDescriptor)},
          expression(move(expression)), outDataPos{outDataPos}, expressionEvaluator{
                                                                    move(expressionEvaluator)} {}

    inline PhysicalOperatorType getOperatorType() override { return PhysicalOperatorType::UNWIND; }

    bool getNextTuples() override;

    shared_ptr<ResultSet> init(ExecutionContext* context) override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<Unwind>(id, paramsString, expression, resultSetDescriptor->copy(),
            outDataPos, expressionEvaluator->clone());
    }

private:
    shared_ptr<Expression> expression;
    DataPos outDataPos;

    unique_ptr<BaseExpressionEvaluator> expressionEvaluator;
    shared_ptr<DataChunk> outDataChunk;
    bool expr_evaluated = false;
};

} // namespace processor
} // namespace graphflow

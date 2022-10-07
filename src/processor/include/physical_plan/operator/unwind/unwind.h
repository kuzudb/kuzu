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
    Unwind(shared_ptr<Expression> expression, unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        DataPos outDataPos, unique_ptr<BaseExpressionEvaluator> expressionEvaluator, uint32_t id,
        const string& paramsString)
        : PhysicalOperator{id, paramsString}, SourceOperator{move(resultSetDescriptor)},
          expression(move(expression)), outDataPos{outDataPos},
          expressionEvaluator{move(expressionEvaluator)}, isExprEvaluated{false}, currentIndex{0u} {
    }

    inline PhysicalOperatorType getOperatorType() override { return PhysicalOperatorType::UNWIND; }

    bool getNextTuples() override;

    shared_ptr<ResultSet> init(ExecutionContext* context) override;

    inline unique_ptr<PhysicalOperator> clone() override {
        return make_unique<Unwind>(expression, resultSetDescriptor->copy(), outDataPos,
            expressionEvaluator->clone(), id, paramsString);
    }

private:
    shared_ptr<Expression> expression;
    DataPos outDataPos;

    unique_ptr<BaseExpressionEvaluator> expressionEvaluator;
    shared_ptr<DataChunk> outDataChunk;
    shared_ptr<ValueVector> valueVector;
    bool isExprEvaluated;
    gf_list_t* inputList;
};

} // namespace processor
} // namespace graphflow

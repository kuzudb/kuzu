#pragma once

#include "src/binder/expression/include/expression.h"
#include "src/common/include/vector/value_vector_utils.h"
#include "src/processor/include/physical_plan/operator/physical_operator.h"
#include "src/processor/include/physical_plan/operator/source_operator.h"
#include "src/processor/include/physical_plan/result/result_set.h"

namespace graphflow {
namespace processor {

class Unwind : public PhysicalOperator, public SourceOperator {
public:
    Unwind(uint32_t id, const string& paramsString, shared_ptr<Expression> expression,
        unique_ptr<ResultSetDescriptor> resultSetDescriptor, DataPos outDataPos)
        : PhysicalOperator{id, paramsString}, SourceOperator{move(resultSetDescriptor)},
          expression(move(expression)), outDataPos{outDataPos} {}

    inline PhysicalOperatorType getOperatorType() override { return PhysicalOperatorType::UNWIND; }

    bool getNextTuples() override;

    shared_ptr<ResultSet> init(ExecutionContext* context) override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<Unwind>(
            id, paramsString, expression, resultSetDescriptor->copy(), outDataPos);
    }

private:
    shared_ptr<Expression> expression;
    DataPos outDataPos;
    uint32_t pos = 0;

    shared_ptr<DataChunk> outDataChunk;
    shared_ptr<ValueVector> outValueVector;
};

} // namespace processor
} // namespace graphflow

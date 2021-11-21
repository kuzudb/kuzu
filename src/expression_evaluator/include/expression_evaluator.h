#pragma once

#include <climits>
#include <functional>

#include "src/common/include/data_chunk/data_chunk.h"
#include "src/common/include/expression_type.h"
#include "src/common/include/value.h"
#include "src/common/include/vector/value_vector.h"
#include "src/processor/include/physical_plan/data_pos.h"
#include "src/processor/include/physical_plan/result/result_set.h"

using namespace graphflow::processor;
using namespace graphflow::common;

namespace graphflow {
namespace evaluator {

class ExpressionEvaluator {

public:
    static std::function<void(ValueVector&, ValueVector&)> getUnaryVectorExecuteOperation(
        ExpressionType type);
    static std::function<uint64_t(ValueVector&, sel_t*)> getUnaryVectorSelectOperation(
        ExpressionType type);

    static std::function<void(ValueVector&, ValueVector&, ValueVector&)>
    getBinaryVectorExecuteOperation(ExpressionType type);
    static std::function<uint64_t(ValueVector&, ValueVector&, sel_t*)>
    getBinaryVectorSelectOperation(ExpressionType type);

    // Creates a leaf literal as value vector expression_evaluator expression.
    ExpressionEvaluator(const shared_ptr<ValueVector>& result, ExpressionType expressionType)
        : dataPos{UINT32_MAX, UINT32_MAX}, result{result},
          expressionType{expressionType}, dataType{result->dataType} {}

    // Creates a leaf variable value vector expression_evaluator expression.
    ExpressionEvaluator(const DataPos& dataPos, ExpressionType expressionType, DataType dataType)
        : dataPos{dataPos}, expressionType{expressionType}, dataType{dataType} {}

    virtual ~ExpressionEvaluator() = default;

    virtual void initResultSet(const ResultSet& resultSet, MemoryManager& memoryManager);

    virtual void evaluate() {
        // For leaf expressions, evaluate function performs no op.
    }

    virtual uint64_t select(sel_t* selectedPositions);

    bool isResultFlat();

    virtual unique_ptr<ExpressionEvaluator> clone();

public:
    DataPos dataPos;
    shared_ptr<ValueVector> result;

    // Fields below are needed to clone expressions.
    ExpressionType expressionType;
    DataType dataType;

protected:
    ExpressionEvaluator(ExpressionType expressionType, DataType dataType)
        : dataPos{UINT32_MAX, UINT32_MAX}, expressionType{expressionType}, dataType{dataType} {}

    vector<unique_ptr<ExpressionEvaluator>> childrenExpr;
};

} // namespace evaluator
} // namespace graphflow

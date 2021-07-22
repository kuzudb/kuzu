#pragma once

#include <climits>
#include <functional>

#include "src/common/include/data_chunk/data_chunk.h"
#include "src/common/include/expression_type.h"
#include "src/common/include/value.h"
#include "src/common/include/vector/value_vector.h"

using namespace graphflow::common;

namespace graphflow {
namespace evaluator {

class ExpressionEvaluator {

public:
    static function<void(ValueVector&, ValueVector&)> getUnaryOperation(ExpressionType type);

    static function<void(ValueVector&, ValueVector&, ValueVector&)> getBinaryOperation(
        ExpressionType type);

    // Creates a leaf literal as value vector expression_evaluator expression.
    ExpressionEvaluator(MemoryManager& memoryManager, const shared_ptr<ValueVector>& result,
        ExpressionType expressionType)
        : result{result}, dataChunkPos{UINT64_MAX}, valueVectorPos{UINT64_MAX},
          expressionType{expressionType}, dataType(result->dataType) {}

    // Creates a leaf variable value vector expression_evaluator expression.
    ExpressionEvaluator(MemoryManager& memoryManager, const shared_ptr<ValueVector>& result,
        uint64_t dataChunkPos, uint64_t valueVectorPos, ExpressionType expressionType)
        : result{result}, dataChunkPos{dataChunkPos}, valueVectorPos{valueVectorPos},
          expressionType{expressionType}, dataType(result->dataType) {}

    virtual void evaluate() {}

    bool isResultFlat();

    inline uint32_t getNumChildrenExpr() const { return childrenExpr.size(); }

    inline const ExpressionEvaluator& getChildExpr(uint64_t pos) const {
        return *childrenExpr[pos];
    }

public:
    shared_ptr<ValueVector> result;
    uint64_t dataChunkPos;
    uint64_t valueVectorPos;
    // expression type is needed to clone unary and binary expressions.
    ExpressionType expressionType;
    DataType dataType;

protected:
    ExpressionEvaluator(ExpressionType expressionType, DataType dataType)
        : dataChunkPos{UINT64_MAX}, valueVectorPos{UINT64_MAX},
          expressionType{expressionType}, dataType{dataType} {}

    vector<unique_ptr<ExpressionEvaluator>> childrenExpr;
};

} // namespace evaluator
} // namespace graphflow

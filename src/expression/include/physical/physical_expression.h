#pragma once

#include <climits>

#include "src/common/include/data_chunk/data_chunk.h"
#include "src/common/include/literal.h"
#include "src/common/include/vector/value_vector.h"

using namespace graphflow::common;

namespace graphflow {
namespace expression {

class PhysicalExpression {

public:
    // Creates a leaf literal as value vector physical expression.
    PhysicalExpression(shared_ptr<ValueVector> result, ExpressionType expressionType)
        : result{result}, valueVectorPos{UINT64_MAX}, expressionType{expressionType},
          dataType(result->dataType) {}

    // Creates a leaf variable value vector physical expression.
    PhysicalExpression(
        shared_ptr<ValueVector> result, uint64_t dataChunkPos, uint64_t valueVectorPos)
        : result{result}, dataChunkPos{dataChunkPos}, valueVectorPos{valueVectorPos},
          expressionType{PROPERTY}, dataType(result->dataType) {}

    virtual void evaluate() {}

    bool isResultFlat();

    inline uint32_t getNumChildrenExpr() const { return childrenExpr.size(); }

    inline const PhysicalExpression& getChildExpr(uint64_t pos) const { return *childrenExpr[pos]; }

public:
    shared_ptr<ValueVector> result;
    uint64_t dataChunkPos;
    uint64_t valueVectorPos;
    // expression type is needed to clone unary and binary expressions.
    ExpressionType expressionType;
    DataType dataType;

protected:
    PhysicalExpression() = default;

    vector<unique_ptr<PhysicalExpression>> childrenExpr;
};

} // namespace expression
} // namespace graphflow

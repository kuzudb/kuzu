#pragma once

#include <climits>

#include "src/common/include/vector/value_vector.h"

using namespace graphflow::common;

namespace graphflow {
namespace expression {

class PhysicalExpression {

public:
    // Creates a leaf literal as value vector physical expression.
    PhysicalExpression(shared_ptr<ValueVector> result, ExpressionType expressionType)
        : PhysicalExpression(result, ULONG_MAX, ULONG_MAX, expressionType){};

    // Creates a leaf variable value vector physical expression.
    PhysicalExpression(shared_ptr<ValueVector> result, uint64_t dataChunkPos,
        uint64_t valueVectorPos, ExpressionType expressionType)
        : result{result}, dataChunkPos{dataChunkPos}, valueVectorPos{valueVectorPos},
          expressionType{expressionType} {};

    virtual void evaluate(){};

    bool isResultFlat();

    virtual void setExpressionResultOwners(shared_ptr<DataChunk> dataChunk) {}

    shared_ptr<ValueVector> createResultValueVector(DataType dataType);

    uint32_t getNumChildrenExpr() const { return childrenExpr.size(); }

    bool isLiteralLeafExpression() const {
        return childrenExpr.size() == 0 && dataChunkPos == ULONG_MAX;
    }

    bool isPropertyLeafExpression() const {
        return childrenExpr.size() == 0 && dataChunkPos != ULONG_MAX;
    }

    inline const PhysicalExpression& getChildExpr(uint64_t pos) const { return *childrenExpr[pos]; }

public:
    shared_ptr<ValueVector> result;
    uint64_t dataChunkPos;
    uint64_t valueVectorPos;
    // expression type is needed to clone unary and binary expressions.
    ExpressionType expressionType;

protected:
    PhysicalExpression() = default;
    vector<unique_ptr<PhysicalExpression>> childrenExpr;
    vector<shared_ptr<ValueVector>> operands;
};

} // namespace expression
} // namespace graphflow

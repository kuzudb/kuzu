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
    PhysicalExpression(Literal literal, ExpressionType expressionType, DataType dataType)
        : literal{literal}, valueVectorPos{UINT64_MAX},
          expressionType{expressionType}, dataType{dataType} {
        result = createResultValueVector(literal.type);
        fillVectorWithLiteralValue(*result, literal);
    }

    // Creates a leaf variable value vector physical expression.
    PhysicalExpression(uint64_t valueVectorPos, DataType dataType)
        : valueVectorPos{valueVectorPos}, expressionType{PROPERTY}, dataType{dataType} {}

    virtual void evaluate() {}

    bool isResultFlat();

    void fillVectorWithLiteralValue(ValueVector& vector, Literal& literalValue) {
        switch (literalValue.type) {
        case INT32: {
            vector.fillValue(literalValue.primitive.integer_);
        } break;
        case DOUBLE: {
            vector.fillValue(literalValue.primitive.double_);
        } break;
        case BOOL:
            vector.fillValue(literalValue.primitive.boolean_);
            break;
        default:
            throw std::invalid_argument("Unsupported data type for literal expression.");
        }
    }

    virtual void setExpressionInputDataChunk(shared_ptr<DataChunk> dataChunk) {
        if (expressionType == PROPERTY) {
            result = dataChunk->getValueVector(valueVectorPos);
        }
    }

    virtual void setExpressionResultOwnerState(shared_ptr<DataChunkState> dataChunkState) {
        result->setDataChunkOwnerState(dataChunkState);
    }

    shared_ptr<ValueVector> createResultValueVector(DataType dataType);

    inline uint32_t getNumChildrenExpr() const { return childrenExpr.size(); }

    inline bool isLiteralLeafExpression() const { return isExpressionLeafLiteral(expressionType); }

    inline bool isPropertyLeafExpression() const { return expressionType == PROPERTY; }

    inline const PhysicalExpression& getChildExpr(uint64_t pos) const { return *childrenExpr[pos]; }

public:
    shared_ptr<ValueVector> result;
    Literal literal;
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

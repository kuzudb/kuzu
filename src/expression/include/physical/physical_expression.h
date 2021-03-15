#pragma once

#include "src/common/include/vector/value_vector.h"

using namespace graphflow::common;

namespace graphflow {
namespace expression {

class PhysicalExpression {

public:
    PhysicalExpression(){};

    PhysicalExpression(shared_ptr<ValueVector> result) : result{move(result)} {};

    virtual void evaluate(){};

    bool isResultFlat();

    shared_ptr<ValueVector> getResult() { return result; }

    DataType getResultDataType() { return result->getDataType(); }

    shared_ptr<ValueVector> createResultValueVector(DataType dataType);

protected:
    vector<unique_ptr<PhysicalExpression>> childrenExpr;
    vector<shared_ptr<ValueVector>> operands;
    shared_ptr<ValueVector> result;
};

} // namespace expression
} // namespace graphflow

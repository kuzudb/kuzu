#pragma once

#include "src/common/include/vector/value_vector.h"

using namespace graphflow::common;

namespace graphflow {
namespace expression {

class PhysicalExpression {

public:
    PhysicalExpression() = default;

    virtual void evaluate() = 0;

    virtual void initExpressionOperation() = 0;

    virtual ValueVector* getResult() = 0;
};

} // namespace expression
} // namespace graphflow

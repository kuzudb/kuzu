#pragma once

#include "include/physical/physical_expression.h"

namespace graphflow {
namespace expression {

class PhysicalUnaryLeafExpression : public PhysicalExpression {

public:
    PhysicalUnaryLeafExpression(shared_ptr<ValueVector> result) : result{result} {}

    inline void evaluate() override{};

    ValueVector* getResult() { return result.get(); }

protected:
    shared_ptr<ValueVector> result;
};

} // namespace expression
} // namespace graphflow

#pragma once

#include "src/processor/include/physical_plan/operator/physical_operator.h"

namespace graphflow {
namespace processor {

class Sink : public PhysicalOperator {

public:
    Sink(unique_ptr<PhysicalOperator> prevOperator, PhysicalOperatorType operatorType,
        ExecutionContext& context, uint32_t id)
        : PhysicalOperator{move(prevOperator), operatorType, context, id} {
        resultSet = this->prevOperator->getResultSet();
    };

    virtual void finalize() {}
};

} // namespace processor
} // namespace graphflow

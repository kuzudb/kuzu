#pragma once

#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

class Sink : public PhysicalOperator {

public:
    Sink(unique_ptr<PhysicalOperator> child, uint32_t id, const string& paramsString)
        : PhysicalOperator{move(child), id, paramsString} {}

    PhysicalOperatorType getOperatorType() override = 0;

    virtual void execute(ExecutionContext* context) = 0;

    bool getNextTuples() final {
        throw InternalException("getNextTuple() should not be called on sink operator.");
    }

    virtual void finalize(ExecutionContext* context){};

    unique_ptr<PhysicalOperator> clone() override = 0;
};

} // namespace processor
} // namespace kuzu

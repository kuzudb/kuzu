#pragma once

#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

class Sink : public PhysicalOperator {
public:
    Sink(uint32_t id, const string& paramsString) : PhysicalOperator{id, paramsString} {}
    Sink(unique_ptr<PhysicalOperator> child, uint32_t id, const string& paramsString)
        : PhysicalOperator{std::move(child), id, paramsString} {}

    PhysicalOperatorType getOperatorType() override = 0;

    inline void execute(ExecutionContext* context) {
        init(context);
        metrics->executionTime.start();
        executeInternal(context);
        metrics->executionTime.stop();
    }

    virtual void finalize(ExecutionContext* context){};

    unique_ptr<PhysicalOperator> clone() override = 0;

protected:
    virtual void executeInternal(ExecutionContext* context) = 0;

    bool getNextTuplesInternal() final {
        throw InternalException("getNextTupleInternal() should not be called on sink operator.");
    }
};

} // namespace processor
} // namespace kuzu

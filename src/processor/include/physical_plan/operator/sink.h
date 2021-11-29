#pragma once

#include "src/processor/include/physical_plan/operator/physical_operator.h"

namespace graphflow {
namespace processor {

class Sink : public PhysicalOperator {

public:
    Sink(shared_ptr<ResultSet> resultSet, unique_ptr<PhysicalOperator> prevOperator,
        PhysicalOperatorType operatorType, ExecutionContext& context, uint32_t id)
        : PhysicalOperator{move(prevOperator), operatorType, context, id} {
        this->resultSet = move(resultSet);
    }

    PhysicalOperator* getPipelineLeafOperator() {
        PhysicalOperator* op = this;
        while (op->prevOperator != nullptr) {
            op = op->prevOperator.get();
        }
        return op;
    }

    virtual void init() { prevOperator->initResultSet(resultSet); }

    // In case there is a sub-plan, the pipeline under sink might be executed repeatedly and thus
    // require a re-initialization after each execution.
    virtual void execute() { reInitialize(); };

    bool getNextTuples() final {
        throw invalid_argument("Sink operator should implement execute instead of getNextTuples");
    }

    virtual void finalize() {}
};

} // namespace processor
} // namespace graphflow

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

    virtual void init() { prevOperator->initResultSet(resultSet); }

    virtual void execute() = 0;

    bool getNextTuples() final {
        throw invalid_argument("Sink operator should implement execute instead of getNextTuples");
    }

    virtual void finalize() {}
};

} // namespace processor
} // namespace graphflow

#pragma once

#include "src/common/include/file_ser_deser_helper.h"
#include "src/processor/include/operator/physical/operator.h"
#include "src/processor/include/operator/physical/tuple/physical_operator_info.h"

using namespace std;

namespace graphflow {
namespace processor {

class LogicalOperator {

public:
    LogicalOperator() = default;

    LogicalOperator(unique_ptr<LogicalOperator> prevOperator) {
        setPrevOperator(move(prevOperator));
    }

    virtual ~LogicalOperator() = default;

    void setPrevOperator(unique_ptr<LogicalOperator> prevOperator) {
        this->prevOperator.reset(prevOperator.release());
    }

    virtual unique_ptr<Operator> mapToPhysical(
        const Graph& graph, PhysicalOperatorsInfo& schema) = 0;

    virtual void serialize(FileSerHelper& fsh) { prevOperator->serialize(fsh); };

protected:
    unique_ptr<LogicalOperator> prevOperator;
};

} // namespace processor
} // namespace graphflow

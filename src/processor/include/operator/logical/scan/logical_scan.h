#pragma once

#include <string>
#include <vector>

#include "src/processor/include/operator/logical/logical_operator.h"

using namespace std;

namespace graphflow {
namespace processor {

class LogicalScan : public LogicalOperator {

public:
    LogicalScan(const string& variableName, const string& label)
        : nodeVarName{variableName}, label{label} {}
    LogicalScan(FileDeserHelper& fdsh);

    unique_ptr<Operator> mapToPhysical(
        const Graph& graph, PhysicalOperatorsInfo& physicalOperatorInfo) override;

    void serialize(FileSerHelper& fsh) override;

protected:
    const string nodeVarName;
    const string label;
};

} // namespace processor
} // namespace graphflow

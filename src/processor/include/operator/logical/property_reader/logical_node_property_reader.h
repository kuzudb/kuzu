#pragma once

#include <string>

#include "src/processor/include/operator/logical/logical_operator.h"

using namespace std;

namespace graphflow {
namespace processor {

class LogicalNodePropertyReader : public LogicalOperator {

public:
    LogicalNodePropertyReader(const string& nodeVarName, const string& nodeLabel,
        const string& propertyName, unique_ptr<LogicalOperator> prevOperator)
        : LogicalOperator{move(prevOperator)}, nodeVarName{nodeVarName}, nodeLabel{nodeLabel},
          propertyName{propertyName} {}
    LogicalNodePropertyReader(FileDeserHelper& fdsh);

    unique_ptr<Operator> mapToPhysical(const Graph& graph, VarToChunkAndVectorIdxMap& schema);

    void serialize(FileSerHelper& fsh) override;

protected:
    const string nodeVarName;
    const string nodeLabel;
    const string propertyName;
};

} // namespace processor
} // namespace graphflow

#pragma once

#include <string>

#include "src/common/include/types.h"
#include "src/processor/include/operator/logical/logical_operator.h"

using namespace graphflow::common;
using namespace std;

namespace graphflow {
namespace processor {

class LogicalRelPropertyReader : public LogicalOperator {

public:
    LogicalRelPropertyReader(const string& fromNodeVarName, const string& fromNodeVarLabel,
        const string& toNodeVarName, const string& nbrNodeVarLabel, const string& relLabel,
        const Direction& direction, const string& propertyName,
        unique_ptr<LogicalOperator> prevOperator)
        : LogicalOperator{move(prevOperator)}, srcNodeVarName{fromNodeVarName},
          srcNodeVarLabel{fromNodeVarLabel}, dstNodeVarName{toNodeVarName},
          dstNodeVarLabel{nbrNodeVarLabel}, relLabel{relLabel}, direction{direction},
          propertyName{propertyName} {}
    LogicalRelPropertyReader(FileDeserHelper& fdsh);

    unique_ptr<Operator> mapToPhysical(const Graph& graph, VarToChunkAndVectorIdxMap& schema);

    void serialize(FileSerHelper& fsh) override;

protected:
    const string srcNodeVarName;
    const string srcNodeVarLabel;
    const string dstNodeVarName;
    const string dstNodeVarLabel;
    const string relLabel;
    const Direction direction;
    const string propertyName;
};

} // namespace processor
} // namespace graphflow

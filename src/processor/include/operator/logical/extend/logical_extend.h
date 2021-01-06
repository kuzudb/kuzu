#pragma once

#include <string>

#include "src/common/include/types.h"
#include "src/processor/include/operator/logical/logical_operator.h"

using namespace graphflow::common;
using namespace std;

namespace graphflow {
namespace processor {

class LogicalExtend : public LogicalOperator {

public:
    LogicalExtend(const string& boundNodeVarName, const string& boundNodeVarLabel,
        const string& nbrNodeVarName, const string& nbrNodeVarLabel, const string& relLabel,
        const Direction& direction, unique_ptr<LogicalOperator> prevOperator)
        : LogicalOperator{move(prevOperator)}, boundNodeVarName{boundNodeVarName},
          boundNodeVarLabel{boundNodeVarLabel}, nbrNodeVarName{nbrNodeVarName},
          nbrNodeVarLabel{nbrNodeVarLabel}, relLabel{relLabel}, direction{direction} {}
    LogicalExtend(FileDeserHelper& fdsh);

    virtual unique_ptr<Operator> mapToPhysical(
        const Graph& graph, VarToChunkAndVectorIdxMap& schema) override;

    virtual void serialize(FileSerHelper& fsh) override;

protected:
    const string boundNodeVarName;
    const string boundNodeVarLabel;
    const string nbrNodeVarName;
    const string nbrNodeVarLabel;
    const string relLabel;
    const Direction direction;
};

} // namespace processor
} // namespace graphflow

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
    LogicalRelPropertyReader(const string& boundNodeVarName, const string& boundNodeVarLabel,
        const string& nbrNodeVarName, const string& nbrNodeVarLabel, const string& relLabel,
        const Direction& direction, const string& propertyName,
        unique_ptr<LogicalOperator> prevOperator)
        : LogicalOperator{move(prevOperator)}, boundNodeVarName{boundNodeVarName},
          boundNodeVarLabel{boundNodeVarLabel}, nbrNodeVarName{nbrNodeVarName},
          nbrNodeVarLabel{nbrNodeVarLabel}, relLabel{relLabel}, direction{direction},
          propertyName{propertyName} {}
    LogicalRelPropertyReader(FileDeserHelper& fdsh);

    unique_ptr<Operator> mapToPhysical(
        const Graph& graph, PhysicalOperatorsInfo& physicalOperatorInfo) override;

    void serialize(FileSerHelper& fsh) override;

protected:
    const string boundNodeVarName;
    const string boundNodeVarLabel;
    const string nbrNodeVarName;
    const string nbrNodeVarLabel;
    const string relLabel;
    const Direction direction;
    const string propertyName;
};

} // namespace processor
} // namespace graphflow

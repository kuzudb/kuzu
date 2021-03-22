#pragma once

#include "src/expression/include/logical/logical_expression.h"
#include "src/expression/include/physical/physical_expression.h"
#include "src/processor/include/physical_plan/operator/tuple/data_chunks.h"
#include "src/processor/include/physical_plan/operator/tuple/physical_operator_info.h"

using namespace graphflow::expression;

namespace graphflow {
namespace processor {

class ExpressionMapper {

public:
    static unique_ptr<PhysicalExpression> mapToPhysical(const LogicalExpression& expression,
        PhysicalOperatorsInfo& physicalOperatorInfo, DataChunks& dataChunks);
};

} // namespace processor
} // namespace graphflow

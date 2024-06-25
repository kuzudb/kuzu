#include "planner/operator/logical_partitioner.h"

#include "binder/expression/expression_util.h"

namespace kuzu {
namespace planner {

void LogicalPartitioner::computeFactorizedSchema() {
    copyChildSchema(0);
}

void LogicalPartitioner::computeFlatSchema() {
    copyChildSchema(0);
}

std::string LogicalPartitioner::getExpressionsForPrinting() const {
    binder::expression_vector expressions;
    for (auto& partitioningInfo : info.partitioningInfos) {
        expressions.push_back(copyFromInfo.columnExprs[partitioningInfo.keyIdx]);
    }
    return binder::ExpressionUtil::toString(expressions);
}

} // namespace planner
} // namespace kuzu

#include "planner/operator/logical_partitioner.h"

#include "binder/expression/expression_util.h"

namespace kuzu {
namespace planner {

std::vector<std::unique_ptr<LogicalPartitionerInfo>> LogicalPartitionerInfo::copy(
    const std::vector<std::unique_ptr<LogicalPartitionerInfo>>& infos) {
    std::vector<std::unique_ptr<LogicalPartitionerInfo>> result;
    result.reserve(infos.size());
    for (auto& info : infos) {
        result.push_back(info->copy());
    }
    return result;
}

void LogicalPartitioner::computeFactorizedSchema() {
    copyChildSchema(0);
}

void LogicalPartitioner::computeFlatSchema() {
    copyChildSchema(0);
}

std::string LogicalPartitioner::getExpressionsForPrinting() const {
    binder::expression_vector expressions;
    for (auto& info : infos) {
        expressions.push_back(copyFromInfo.columnExprs[info->keyIdx]);
    }
    return binder::ExpressionUtil::toString(expressions);
}

} // namespace planner
} // namespace kuzu

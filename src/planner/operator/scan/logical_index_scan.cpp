#include "planner/operator/scan/logical_index_scan.h"

#include "binder/expression/expression_util.h"

namespace kuzu {
namespace planner {

std::string LogicalIndexScanNode::getExpressionsForPrinting() const {
    binder::expression_vector expressions;
    for (auto& info : infos) {
        expressions.push_back(info.offset);
    }
    return binder::ExpressionUtil::toString(expressions);
}

void LogicalIndexScanNode::computeFactorizedSchema() {
    copyChildSchema(0);
    for (auto& info : infos) {
        auto groupPos = 0u;
        if (schema->isExpressionInScope(*info.key)) {
            groupPos = schema->getGroupPos(*info.key);
        }
        schema->insertToGroupAndScope(info.offset, groupPos);
    }
}

void LogicalIndexScanNode::computeFlatSchema() {
    copyChildSchema(0);
    for (auto& info : infos) {
        schema->insertToGroupAndScope(info.offset, 0);
    }
}

} // namespace planner
} // namespace kuzu

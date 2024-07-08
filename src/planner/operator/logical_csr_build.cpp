#include "planner/operator/logical_csr_build.h"

#include "common/exception/not_implemented.h"

namespace kuzu {
namespace planner {

f_group_pos_set LogicalCSRBuild::getGroupsPosToFlatten() {
    f_group_pos_set result;
    auto inSchema = children[0]->getSchema();
    auto srcNodeExpr = ku_dynamic_cast<binder::Expression*, binder::NodeExpression*>(boundNode.get());
    auto boundNodeGroupPos = inSchema->getGroupPos(*srcNodeExpr->getInternalID());
    if (!inSchema->getGroup(boundNodeGroupPos)->isFlat()) {
        result.insert(boundNodeGroupPos);
    }
    return result;
}

void LogicalCSRBuild::computeFlatSchema() {
    copyChildSchema(0);
}

void LogicalCSRBuild::computeFactorizedSchema() {
    copyChildSchema(0);
}

std::string LogicalCSRBuild::getExpressionsForPrinting() const {
    auto result = boundNode->toString();
    result += "->fwd_csr";
    return result;
}

} // namespace planner
} // namespace kuzu

#include "optimizer/logical_operator_collector.h"

namespace kuzu {
namespace optimizer {

void LogicalOperatorCollector::collect(planner::LogicalOperator* op) {
    for (auto i = 0u; i < op->getNumChildren(); ++i) {
        collect(op->getChild(i).get());
    }
    visitOperatorSwitch(op);
}

} // namespace optimizer
} // namespace kuzu

#pragma once

#include "binder/expression/node_expression.h"
#include "logical_plan.h"

namespace kuzu {
namespace planner {

class LogicalPlanUtil {
public:
    // Return the node whose ID has sequential guarantee on the plan.
    static std::shared_ptr<binder::NodeExpression> getSequentialNode(LogicalPlan& plan);

    static inline std::vector<LogicalOperator*> collectOperators(
        const LogicalPlan& plan, LogicalOperatorType operatorType) {
        return collectOperators(plan.getLastOperator().get(), operatorType);
    }

    static std::vector<LogicalOperator*> collectOperators(
        LogicalOperator* root, LogicalOperatorType operatorType);

    static inline std::string encodeJoin(LogicalPlan& logicalPlan) {
        return encodeJoin(logicalPlan.getLastOperator().get());
    }

private:
    static LogicalOperator* getCurrentPipelineSourceOperator(LogicalPlan& plan);

    static void collectOperatorsRecursive(LogicalOperator* op, LogicalOperatorType operatorType,
        std::vector<LogicalOperator*>& result);

    static std::string encodeJoin(LogicalOperator* logicalOperator) {
        std::string result;
        encodeJoinRecursive(logicalOperator, result);
        return result;
    }

    static void encodeJoinRecursive(LogicalOperator* logicalOperator, std::string& encodeString);

    static void encodeCrossProduct(LogicalOperator* logicalOperator, std::string& encodeString);
    static void encodeIntersect(LogicalOperator* logicalOperator, std::string& encodeString);
    static void encodeHashJoin(LogicalOperator* logicalOperator, std::string& encodeString);
    static void encodeExtend(LogicalOperator* logicalOperator, std::string& encodeString);
    static void encodeScanNodeID(LogicalOperator* logicalOperator, std::string& encodeString);
};

} // namespace planner
} // namespace kuzu

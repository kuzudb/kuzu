#pragma once

#include "binder/expression/node_expression.h"
#include "logical_plan.h"

namespace kuzu {
namespace planner {

class LogicalPlanUtil {
public:
    // Return the node whose ID has sequential guarantee on the plan.
    static shared_ptr<NodeExpression> getSequentialNode(LogicalPlan& plan);

    static inline vector<LogicalOperator*> collectOperators(
        const LogicalPlan& plan, LogicalOperatorType operatorType) {
        return collectOperators(plan.getLastOperator().get(), operatorType);
    }

    static vector<LogicalOperator*> collectOperators(
        LogicalOperator* root, LogicalOperatorType operatorType);

    static inline string encodeJoin(LogicalPlan& logicalPlan) {
        return encodeJoin(logicalPlan.getLastOperator().get());
    }

private:
    static LogicalOperator* getCurrentPipelineSourceOperator(LogicalPlan& plan);

    static void collectOperatorsRecursive(
        LogicalOperator* op, LogicalOperatorType operatorType, vector<LogicalOperator*>& result);

    static string encodeJoin(LogicalOperator* logicalOperator) {
        string result;
        encodeJoinRecursive(logicalOperator, result);
        return result;
    }

    static void encodeJoinRecursive(LogicalOperator* logicalOperator, string& encodeString);

    static void encodeCrossProduct(LogicalOperator* logicalOperator, string& encodeString);
    static void encodeIntersect(LogicalOperator* logicalOperator, string& encodeString);
    static void encodeHashJoin(LogicalOperator* logicalOperator, string& encodeString);
    static void encodeExtend(LogicalOperator* logicalOperator, string& encodeString);
    static void encodeScanNodeID(LogicalOperator* logicalOperator, string& encodeString);
};

} // namespace planner
} // namespace kuzu

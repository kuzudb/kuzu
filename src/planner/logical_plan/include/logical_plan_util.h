#pragma once

#include "logical_plan.h"

namespace graphflow {
namespace planner {

class LogicalPlanUtil {
public:
    static vector<LogicalOperator*> collectOperators(
        const LogicalPlan& plan, LogicalOperatorType operatorType);

    static inline string encodeJoin(LogicalPlan& logicalPlan) {
        return encodeJoin(logicalPlan.getLastOperator().get());
    }

private:
    static void collectOperatorsRecursive(
        LogicalOperator* op, LogicalOperatorType operatorType, vector<LogicalOperator*>& result);

    static string encodeJoin(LogicalOperator* logicalOperator) {
        string result;
        encodeJoinRecursive(logicalOperator, result);
        return result;
    }

    static void encodeJoinRecursive(LogicalOperator* logicalOperator, string& encodeString);

    static void encodeHashJoin(LogicalOperator* logicalOperator, string& encodeString);
    static void encodeExtend(LogicalOperator* logicalOperator, string& encodeString);
    static void encodeScanNodeID(LogicalOperator* logicalOperator, string& encodeString);
};

} // namespace planner
} // namespace graphflow

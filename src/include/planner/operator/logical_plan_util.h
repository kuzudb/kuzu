#pragma once

#include "planner/operator/logical_plan.h"

namespace kuzu {
namespace planner {

class LogicalPlanUtil {
public:
    static inline std::string encodeJoin(LogicalPlan& logicalPlan) {
        return encodeJoin(logicalPlan.getLastOperator().get());
    }

private:
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
    static void encodeRecursiveExtend(LogicalOperator* logicalOperator, std::string& encodeString);
    static void encodeScanInternalID(LogicalOperator* logicalOperator, std::string& encodeString);
};

} // namespace planner
} // namespace kuzu

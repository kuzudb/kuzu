#include "planner/operator/logical_limit.h"

#include "binder/expression/expression_util.h"
#include "binder/expression/literal_expression.h"
#include "binder/expression/parameter_expression.h"
#include "common/exception/runtime.h"
#include "common/type_utils.h"
#include "planner/operator/factorization/flatten_resolver.h"

namespace kuzu {
namespace planner {

static uint64_t getLiteralNumber(std::shared_ptr<kuzu::binder::Expression> expr) {
    uint64_t number = common::INVALID_LIMIT;
    if (expr == nullptr) {
        return number;
    }
    auto value = common::Value::createDefaultValue(expr->dataType);
    switch (expr->expressionType) {
    case common::ExpressionType::LITERAL: {
        value = expr->constCast<binder::LiteralExpression>().getValue();
    } break;
    case common::ExpressionType::PARAMETER: {
        value = expr->constCast<binder::ParameterExpression>().getValue();
    } break;
    default:
        KU_UNREACHABLE;
    }
    auto errorMsg = "The number of rows to skip/limit must be a non-negative integer.";
    common::TypeUtils::visit(
        value.getDataType(),
        [&]<common::IntegerTypes T>(T) {
            if (value.getValue<T>() < 0) {
                throw common::RuntimeException{errorMsg};
            }
            number = (uint64_t)value.getValue<T>();
        },
        [&](auto) { throw common::RuntimeException{errorMsg}; });
    return number;
}

static bool canEvaluate(std::shared_ptr<binder::Expression> expr) {
    KU_ASSERT(expr != nullptr);
    return (expr->expressionType == common::ExpressionType::LITERAL ||
            (expr->expressionType == common::ExpressionType::PARAMETER &&
                expr->getDataType().getLogicalTypeID() != common::LogicalTypeID::ANY));
}

bool LogicalLimit::canEvaluateSkipNum() const {
    return canEvaluate(skipNum);
}

uint64_t LogicalLimit::evaluateSkipNum() const {
    return getLiteralNumber(skipNum);
}

bool LogicalLimit::canEvaluateLimitNum() const {
    return canEvaluate(limitNum);
}

uint64_t LogicalLimit::evaluateLimitNum() const {
    return getLiteralNumber(limitNum);
}

std::string LogicalLimit::getExpressionsForPrinting() const {
    std::string result;
    if (hasSkipNum()) {
        result += "SKIP ";
        if (canEvaluateSkipNum()) {
            result += std::to_string(evaluateSkipNum());
        }
    }
    if (hasLimitNum()) {
        if (!result.empty()) {
            result += ",";
        }
        result += "LIMIT ";
        if (canEvaluateLimitNum()) {
            result += std::to_string(evaluateLimitNum());
        }
    }
    return result;
}

f_group_pos_set LogicalLimit::getGroupsPosToFlatten() {
    auto childSchema = children[0]->getSchema();
    return FlattenAllButOne::getGroupsPosToFlatten(childSchema->getGroupsPosInScope(),
        *childSchema);
}

f_group_pos LogicalLimit::getGroupPosToSelect() const {
    auto childSchema = children[0]->getSchema();
    auto groupsPosInScope = childSchema->getGroupsPosInScope();
    SchemaUtils::validateAtMostOneUnFlatGroup(groupsPosInScope, *childSchema);
    return SchemaUtils::getLeadingGroupPos(groupsPosInScope, *childSchema);
}

} // namespace planner
} // namespace kuzu

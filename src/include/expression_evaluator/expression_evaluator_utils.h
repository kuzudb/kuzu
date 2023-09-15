#pragma once

#include "binder/expression/expression.h"
#include "expression_evaluator.h"

namespace kuzu {
namespace evaluator {

struct ExpressionEvaluatorUtils {
    static std::unique_ptr<common::Value> evaluateConstantExpression(
        const std::shared_ptr<binder::Expression>& expression,
        storage::MemoryManager* memoryManager);
};

} // namespace evaluator
} // namespace kuzu

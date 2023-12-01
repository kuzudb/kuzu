#pragma once

#include "binder/expression/expression.h"
#include "common/types/value/value.h"
#include "storage/buffer_manager/memory_manager.h"

namespace kuzu {
namespace evaluator {

struct ExpressionEvaluatorUtils {
    static std::unique_ptr<common::Value> evaluateConstantExpression(
        const std::shared_ptr<binder::Expression>& expression,
        storage::MemoryManager* memoryManager);
};

} // namespace evaluator
} // namespace kuzu

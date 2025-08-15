#pragma once

#include "binder/expression/expression.h"
#include "common/types/value/value.h"

namespace kuzu {
namespace evaluator {

struct ExpressionEvaluatorUtils {
    static KUZU_API common::Value evaluateConstantExpression(
        std::shared_ptr<binder::Expression> expression, main::ClientContext* clientContext);
};

} // namespace evaluator
} // namespace kuzu

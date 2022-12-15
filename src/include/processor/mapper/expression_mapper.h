#pragma once

#include "binder/expression/expression.h"
#include "expression_evaluator/base_evaluator.h"
#include "processor/execution_context.h"
#include "processor/result/result_set.h"
#include "processor/result/result_set_descriptor.h"

using namespace kuzu::binder;
using namespace kuzu::evaluator;

namespace kuzu {
namespace processor {

class PlanMapper;

class ExpressionMapper {

public:
    unique_ptr<BaseExpressionEvaluator> mapExpression(
        const shared_ptr<Expression>& expression, const Schema& schema);

private:
    unique_ptr<BaseExpressionEvaluator> mapLiteralExpression(
        const shared_ptr<Expression>& expression);

    unique_ptr<BaseExpressionEvaluator> mapParameterExpression(
        const shared_ptr<Expression>& expression);

    unique_ptr<BaseExpressionEvaluator> mapReferenceExpression(
        const shared_ptr<Expression>& expression, const Schema& schema);

    unique_ptr<BaseExpressionEvaluator> mapCaseExpression(
        const shared_ptr<Expression>& expression, const Schema& schema);

    unique_ptr<BaseExpressionEvaluator> mapFunctionExpression(
        const shared_ptr<Expression>& expression, const Schema& schema);
};

} // namespace processor
} // namespace kuzu

#include "binder/expression/expression_util.h"
#include "binder/expression/node_expression.h"
#include "binder/expression/rel_expression.h"
#include "binder/expression_binder.h"
#include "function/rewrite_function.h"
#include "function/schema/vector_node_rel_functions.h"

using namespace kuzu::common;
using namespace kuzu::binder;

namespace kuzu {
namespace function {

static std::shared_ptr<binder::Expression> rewriteFunc(
    const expression_vector& params, ExpressionBinder* binder) {
    KU_ASSERT(params.size() == 1);
    auto param = params[0].get();
    if (ExpressionUtil::isNodePattern(*param)) {
        auto node = ku_dynamic_cast<Expression*, NodeExpression*>(param);
        return node->getInternalID();
    }
    if (ExpressionUtil::isRelPattern(*param)) {
        auto rel = ku_dynamic_cast<Expression*, RelExpression*>(param);
        return rel->getPropertyExpression(InternalKeyword::ID);
    }
    // Bind as struct_extract(param, "_id")
    auto key = Value(LogicalType::STRING(), InternalKeyword::ID);
    auto keyExpr = binder->createLiteralExpression(key.copy());
    auto newParams = expression_vector{params[0], keyExpr};
    return binder->bindScalarFunctionExpression(newParams, STRUCT_EXTRACT_FUNC_NAME);
}

function_set IDFunction::getFunctionSet() {
    function_set functionSet;
    auto inputTypes =
        std::vector<LogicalTypeID>{LogicalTypeID::NODE, LogicalTypeID::REL, LogicalTypeID::STRUCT};
    for (auto& inputType : inputTypes) {
        auto function = std::make_unique<RewriteFunction>(
            InternalKeyword::ID, std::vector<LogicalTypeID>{inputType}, rewriteFunc);
        functionSet.push_back(std::move(function));
    }
    return functionSet;
}

} // namespace function
} // namespace kuzu

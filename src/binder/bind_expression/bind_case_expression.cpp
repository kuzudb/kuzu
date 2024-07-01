#include "binder/binder.h"
#include "binder/expression/case_expression.h"
#include "binder/expression_binder.h"
#include "common/cast.h"
#include "parser/expression/parsed_case_expression.h"

using namespace kuzu::common;
using namespace kuzu::parser;

namespace kuzu {
namespace binder {

std::shared_ptr<Expression> ExpressionBinder::bindCaseExpression(
    const ParsedExpression& parsedExpression) {
    auto& parsedCaseExpression =
        ku_dynamic_cast<const ParsedExpression&, const ParsedCaseExpression&>(parsedExpression);
    auto resultType = LogicalType();
    // Resolve result type by checking each then expression type.
    for (auto i = 0u; i < parsedCaseExpression.getNumCaseAlternative(); ++i) {
        auto alternative = parsedCaseExpression.getCaseAlternative(i);
        auto boundThen = bindExpression(*alternative->thenExpression);
        if (boundThen->getDataType().getLogicalTypeID() != LogicalTypeID::ANY) {
            resultType = boundThen->getDataType().copy();
        }
    }
    // Resolve result type by else expression if above resolving fails.
    if (resultType.getLogicalTypeID() == LogicalTypeID::ANY &&
        parsedCaseExpression.hasElseExpression()) {
        auto elseExpression = bindExpression(*parsedCaseExpression.getElseExpression());
        resultType = elseExpression->getDataType().copy();
    }
    auto name = binder->getUniqueExpressionName(parsedExpression.getRawName());
    // bind ELSE ...
    std::shared_ptr<Expression> elseExpression;
    if (parsedCaseExpression.hasElseExpression()) {
        elseExpression = bindExpression(*parsedCaseExpression.getElseExpression());
    } else {
        elseExpression = createNullLiteralExpression();
    }
    elseExpression = implicitCastIfNecessary(elseExpression, resultType);
    auto boundCaseExpression =
        make_shared<CaseExpression>(resultType.copy(), std::move(elseExpression), name);
    // bind WHEN ... THEN ...
    if (parsedCaseExpression.hasCaseExpression()) {
        auto boundCase = bindExpression(*parsedCaseExpression.getCaseExpression());
        for (auto i = 0u; i < parsedCaseExpression.getNumCaseAlternative(); ++i) {
            auto caseAlternative = parsedCaseExpression.getCaseAlternative(i);
            auto boundWhen = bindExpression(*caseAlternative->whenExpression);
            boundWhen = implicitCastIfNecessary(boundWhen, boundCase->dataType);
            // rewrite "CASE a.age WHEN 1" as "CASE WHEN a.age = 1"
            boundWhen = bindComparisonExpression(ExpressionType::EQUALS,
                std::vector<std::shared_ptr<Expression>>{boundCase, boundWhen});
            auto boundThen = bindExpression(*caseAlternative->thenExpression);
            boundThen = implicitCastIfNecessary(boundThen, resultType);
            boundCaseExpression->addCaseAlternative(boundWhen, boundThen);
        }
    } else {
        for (auto i = 0u; i < parsedCaseExpression.getNumCaseAlternative(); ++i) {
            auto caseAlternative = parsedCaseExpression.getCaseAlternative(i);
            auto boundWhen = bindExpression(*caseAlternative->whenExpression);
            boundWhen = implicitCastIfNecessary(boundWhen, LogicalType::BOOL());
            auto boundThen = bindExpression(*caseAlternative->thenExpression);
            boundThen = implicitCastIfNecessary(boundThen, resultType);
            boundCaseExpression->addCaseAlternative(boundWhen, boundThen);
        }
    }
    return boundCaseExpression;
}

} // namespace binder
} // namespace kuzu

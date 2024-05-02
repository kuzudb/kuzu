#include "binder/binder.h"
#include "binder/expression/expression_util.h"
#include "binder/query/reading_clause/bound_unwind_clause.h"
#include "parser/query/reading_clause/unwind_clause.h"

using namespace kuzu::parser;
using namespace kuzu::common;

namespace kuzu {
namespace binder {

// E.g. UNWIND $1. We cannot validate $1 has data type LIST until we see the actual parameter.
static bool skipDataTypeValidation(const Expression& expr) {
    return expr.expressionType == ExpressionType::PARAMETER &&
           expr.getDataType().getLogicalTypeID() == LogicalTypeID::ANY;
}

std::unique_ptr<BoundReadingClause> Binder::bindUnwindClause(const ReadingClause& readingClause) {
    auto& unwindClause = readingClause.constCast<UnwindClause>();
    auto boundExpression = expressionBinder.bindExpression(*unwindClause.getExpression());
    auto aliasName = unwindClause.getAlias();
    std::shared_ptr<Expression> alias;
    if (boundExpression->getDataType().getLogicalTypeID() == LogicalTypeID::ARRAY) {
        auto targetType = LogicalType::LIST(ArrayType::getChildType(boundExpression->dataType));
        boundExpression = expressionBinder.implicitCast(boundExpression, *targetType);
    }
    if (!skipDataTypeValidation(*boundExpression)) {
        ExpressionUtil::validateDataType(*boundExpression, LogicalTypeID::LIST);
        alias = createVariable(aliasName, ListType::getChildType(boundExpression->dataType));
    } else {
        alias = createVariable(aliasName, *LogicalType::ANY());
    }
    std::shared_ptr<Expression> idExpr = nullptr;
    if (scope.hasMemorizedTableIDs(boundExpression->getAlias())) {
        auto tableIDs = scope.getMemorizedTableIDs(boundExpression->getAlias());
        auto node = createQueryNode(aliasName, tableIDs);
        idExpr = node->getInternalID();
        scope.addNodeReplacement(node);
    }
    return make_unique<BoundUnwindClause>(std::move(boundExpression), std::move(alias),
        std::move(idExpr));
}

} // namespace binder
} // namespace kuzu

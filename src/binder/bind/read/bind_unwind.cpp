#include "binder/binder.h"
#include "binder/expression/expression_util.h"
#include "binder/query/reading_clause/bound_unwind_clause.h"
#include "parser/query/reading_clause/unwind_clause.h"

using namespace kuzu::parser;
using namespace kuzu::common;

namespace kuzu {
namespace binder {

std::unique_ptr<BoundReadingClause> Binder::bindUnwindClause(const ReadingClause& readingClause) {
    auto& unwindClause = readingClause.constCast<UnwindClause>();
    auto boundExpression = expressionBinder.bindExpression(*unwindClause.getExpression());
    ExpressionUtil::validateDataType(*boundExpression, LogicalTypeID::LIST);
    auto aliasName = unwindClause.getAlias();
    auto alias = createVariable(aliasName, *ListType::getChildType(&boundExpression->dataType));
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

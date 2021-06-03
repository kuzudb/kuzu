#include "test/binder/binder_test_utils.h"

bool BinderTestUtils::equals(
    const BoundLoadCSVStatement& left, const BoundLoadCSVStatement& right) {
    auto result = left.filePath == right.filePath && left.tokenSeparator == right.tokenSeparator &&
                  left.csvColumnVariables.size() == right.csvColumnVariables.size();
    if (!result) {
        return false;
    }
    for (auto i = 0u; i < left.csvColumnVariables.size(); ++i) {
        if (!equals(*left.csvColumnVariables[i], *right.csvColumnVariables[i])) {
            return false;
        }
    }
    return true;
}

bool BinderTestUtils::equals(const Expression& left, const Expression& right) {
    auto result = left.expressionType == right.expressionType && left.dataType == right.dataType &&
                  left.variableName == right.variableName && left.alias == right.alias &&
                  left.childrenExpr.size() == right.childrenExpr.size();
    if (!result) {
        return false;
    }
    for (auto i = 0u; i < left.childrenExpr.size(); ++i) {
        if (!equals(*left.childrenExpr[i], *right.childrenExpr[i])) {
            return false;
        }
    }
    if (PROPERTY == left.expressionType) {
        return equals((PropertyExpression&)left, (PropertyExpression&)right);
    } else if (VARIABLE == left.expressionType) {
        if (NODE == left.dataType) {
            return equals((NodeExpression&)left, (NodeExpression&)right);
        } else if (REL == left.dataType) {
            return equals((RelExpression&)left, (RelExpression&)right);
        } else {
            return true;
        }
    } else if (isExpressionLeafLiteral(left.expressionType)) {
        return equals((LiteralExpression&)left, (LiteralExpression&)right);
    }
    return true;
}

bool BinderTestUtils::equals(const PropertyExpression& left, const PropertyExpression& right) {
    return left.propertyKey == right.propertyKey;
}

bool BinderTestUtils::equals(const RelExpression& left, const RelExpression& right) {
    return left.label == right.label && equals(*left.srcNode, *right.srcNode) &&
           equals(*left.dstNode, *right.dstNode);
}

bool BinderTestUtils::equals(const NodeExpression& left, const NodeExpression& right) {
    return left.label == right.label;
}

bool BinderTestUtils::equals(const LiteralExpression& left, const LiteralExpression& right) {
    return left.literal.toString() == right.literal.toString();
}

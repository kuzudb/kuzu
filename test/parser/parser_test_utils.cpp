#include "test/parser/parser_test_utils.h"

bool ParserTestUtils::equals(const MatchStatement& left, const MatchStatement& right) {
    auto result = left.graphPattern.size() == right.graphPattern.size();
    if (left.whereClause) {
        result &= right.whereClause && equals(*left.whereClause, *right.whereClause);
    }
    if (!result) {
        return false;
    }
    for (auto i = 0u; i < left.graphPattern.size(); ++i) {
        if (!equals(*left.graphPattern[i], *right.graphPattern[i])) {
            return false;
        }
    }
    return true;
}

bool ParserTestUtils::equals(const WithStatement& left, const WithStatement& right) {
    auto result = equals(*left.getProjectionBody(), *right.getProjectionBody());
    if (left.hasWhereExpression()) {
        result &= right.hasWhereExpression() &&
                  equals(*left.getWhereExpression(), *right.getWhereExpression());
    }
    return result;
}

bool ParserTestUtils::equals(const ReturnStatement& left, const ReturnStatement& right) {
    return equals(*left.getProjectionBody(), *right.getProjectionBody());
}

bool ParserTestUtils::equals(const ParsedExpression& left, const ParsedExpression& right) {
    auto result = left.type == right.type && left.text == right.text && left.alias == right.alias &&
                  left.children.size() == right.children.size();
    if (!result)
        return false;
    for (auto i = 0u; i < left.children.size(); ++i) {
        if (!equals(*left.children[i], *right.children[i])) {
            return false;
        }
    }
    return true;
}

bool ParserTestUtils::equals(const SingleQuery& left, const SingleQuery& right) {
    auto result = left.queryParts.size() == right.queryParts.size() &&
                  left.matchStatements.size() == right.matchStatements.size() &&
                  equals(*left.returnStatement, *right.returnStatement);
    if (!result)
        return false;
    for (auto i = 0u; i < left.queryParts.size(); ++i) {
        if (!equals(*left.queryParts[i], *right.queryParts[i])) {
            return false;
        }
    }
    for (auto i = 0u; i < left.matchStatements.size(); ++i) {
        if (!equals(*left.matchStatements[i], *right.matchStatements[i])) {
            return false;
        }
    }
    return true;
}

bool ParserTestUtils::equals(const QueryPart& left, const QueryPart& right) {
    auto result = left.matchStatements.size() == right.matchStatements.size() &&
                  equals(*left.withStatement, *right.withStatement);
    if (!result)
        return false;
    for (auto i = 0u; i < left.matchStatements.size(); ++i) {
        if (!equals(*left.matchStatements[i], *right.matchStatements[i])) {
            return false;
        }
    }
    return true;
}

bool ParserTestUtils::equals(const ProjectionBody& left, const ProjectionBody& right) {
    auto result = left.isProjectStar() == right.isProjectStar() &&
                  equals(left.getProjectionExpressions(), right.getProjectionExpressions());
    if (left.hasLimitExpression()) {
        result &= right.hasLimitExpression() &&
                  equals(*left.getLimitExpression(), *right.getLimitExpression());
    }
    return result;
}

bool ParserTestUtils::equals(const vector<unique_ptr<ParsedExpression>>& left,
    const vector<unique_ptr<ParsedExpression>>& right) {
    auto result = left.size() == right.size();
    if (!result)
        return false;
    for (auto i = 0u; i < left.size(); ++i) {
        if (!equals(*left[i], *right[i])) {
            return false;
        }
    }
    return true;
}

bool ParserTestUtils::equals(const PatternElement& left, const PatternElement& right) {
    auto result = equals(*left.nodePattern, *right.nodePattern) &&
                  left.patternElementChains.size() == right.patternElementChains.size();
    if (!result)
        return false;
    for (auto i = 0u; i < left.patternElementChains.size(); ++i) {
        if (!equals(*left.patternElementChains[i], *right.patternElementChains[i])) {
            return false;
        }
    }
    return true;
}

bool ParserTestUtils::equals(const PatternElementChain& left, const PatternElementChain& right) {
    return equals(*left.nodePattern, *right.nodePattern) &&
           equals(*left.relPattern, *right.relPattern);
}

bool ParserTestUtils::equals(const RelPattern& left, const RelPattern& right) {
    return left.name == right.name && left.label == right.label &&
           left.arrowDirection == right.arrowDirection;
}

bool ParserTestUtils::equals(const NodePattern& left, const NodePattern& right) {
    return left.name == right.name && left.label == right.label;
}

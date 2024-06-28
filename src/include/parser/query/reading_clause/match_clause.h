#pragma once

#include "parser/expression/parsed_expression.h"
#include "parser/query/graph_pattern/pattern_element.h"
#include "reading_clause.h"

namespace kuzu {
namespace parser {

struct JoinHintNode {
    std::string variableName;
    std::vector<std::shared_ptr<JoinHintNode>> children;

    JoinHintNode() = default;
    explicit JoinHintNode(std::string name) : variableName{std::move(name)} {}
    void addChild(std::shared_ptr<JoinHintNode> child) { children.push_back(std::move(child)); }
    bool isLeaf() const { return children.empty(); }
};

class MatchClause : public ReadingClause {
    static constexpr common::ClauseType clauseType_ = common::ClauseType::MATCH;

public:
    MatchClause(std::vector<PatternElement> patternElements,
        common::MatchClauseType matchClauseType)
        : ReadingClause{clauseType_}, patternElements{std::move(patternElements)},
          matchClauseType{matchClauseType} {}

    const std::vector<PatternElement>& getPatternElementsRef() const { return patternElements; }

    void setWherePredicate(std::unique_ptr<ParsedExpression> expression) {
        wherePredicate = std::move(expression);
    }
    bool hasWherePredicate() const { return wherePredicate != nullptr; }
    const ParsedExpression* getWherePredicate() const { return wherePredicate.get(); }

    common::MatchClauseType getMatchClauseType() const { return matchClauseType; }

    void setHint(std::shared_ptr<JoinHintNode> root) { hintRoot = std::move(root); }
    bool hasHint() const { return hintRoot != nullptr; }
    std::shared_ptr<JoinHintNode> getHint() const { return hintRoot; }

private:
    std::vector<PatternElement> patternElements;
    std::unique_ptr<ParsedExpression> wherePredicate;
    common::MatchClauseType matchClauseType;
    std::shared_ptr<JoinHintNode> hintRoot = nullptr;
};

} // namespace parser
} // namespace kuzu

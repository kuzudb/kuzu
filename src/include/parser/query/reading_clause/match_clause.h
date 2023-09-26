#pragma once

#include "parser/expression/parsed_expression.h"
#include "parser/query/graph_pattern/pattern_element.h"
#include "reading_clause.h"

namespace kuzu {
namespace parser {

class MatchClause : public ReadingClause {
public:
    MatchClause(std::vector<std::unique_ptr<PatternElement>> patternElements,
        common::MatchClauseType matchClauseType)
        : ReadingClause{common::ClauseType::MATCH}, patternElements{std::move(patternElements)},
          matchClauseType{matchClauseType} {}
    ~MatchClause() override = default;

    inline const std::vector<std::unique_ptr<PatternElement>>& getPatternElements() const {
        return patternElements;
    }

    inline void setWherePredicate(std::unique_ptr<ParsedExpression> expression) {
        wherePredicate = std::move(expression);
    }
    inline bool hasWherePredicate() const { return wherePredicate != nullptr; }
    inline ParsedExpression* getWherePredicate() const { return wherePredicate.get(); }

    inline common::MatchClauseType getMatchClauseType() const { return matchClauseType; }

private:
    std::vector<std::unique_ptr<PatternElement>> patternElements;
    std::unique_ptr<ParsedExpression> wherePredicate;
    common::MatchClauseType matchClauseType;
};

} // namespace parser
} // namespace kuzu

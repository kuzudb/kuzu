#pragma once

#include "parsed_expression.h"
#include "parser/query/reading_clause/match_clause.h"

namespace kuzu {
namespace parser {

class ParsedSubqueryExpression : public ParsedExpression {
public:
    ParsedSubqueryExpression(
        std::vector<std::unique_ptr<PatternElement>> patternElements, std::string rawName)
        : ParsedExpression{common::EXISTENTIAL_SUBQUERY, std::move(rawName)},
          patternElements{std::move(patternElements)} {}

    inline const std::vector<std::unique_ptr<PatternElement>>& getPatternElements() const {
        return patternElements;
    }

    inline void setWhereClause(std::unique_ptr<ParsedExpression> expression) {
        whereClause = std::move(expression);
    }
    inline bool hasWhereClause() const { return whereClause != nullptr; }
    inline ParsedExpression* getWhereClause() const { return whereClause.get(); }

private:
    std::vector<std::unique_ptr<PatternElement>> patternElements;
    std::unique_ptr<ParsedExpression> whereClause;
};

} // namespace parser
} // namespace kuzu

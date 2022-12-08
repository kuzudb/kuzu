#pragma once

#include "parsed_expression.h"
#include "parser/query/reading_clause/match_clause.h"

namespace kuzu {
namespace parser {

class ParsedSubqueryExpression : public ParsedExpression {
public:
    ParsedSubqueryExpression(vector<unique_ptr<PatternElement>> patternElements, string rawName)
        : ParsedExpression{EXISTENTIAL_SUBQUERY, std::move(rawName)}, patternElements{std::move(
                                                                          patternElements)} {}

    inline const vector<unique_ptr<PatternElement>>& getPatternElements() const {
        return patternElements;
    }

    inline void setWhereClause(unique_ptr<ParsedExpression> expression) {
        whereClause = std::move(expression);
    }
    inline bool hasWhereClause() const { return whereClause != nullptr; }
    inline ParsedExpression* getWhereClause() const { return whereClause.get(); }

private:
    vector<unique_ptr<PatternElement>> patternElements;
    unique_ptr<ParsedExpression> whereClause;
};

} // namespace parser
} // namespace kuzu

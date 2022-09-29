#pragma once

#include "parsed_expression.h"

#include "src/parser/query/reading_clause/include/match_clause.h"

namespace graphflow {
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

    bool equals(const ParsedExpression& other) const override {
        auto& otherSubquery = (ParsedSubqueryExpression&)other;
        if (!ParsedExpression::equals(other) ||
            patternElements.size() != otherSubquery.patternElements.size()) {
            return false;
        }
        if (hasWhereClause() && *whereClause != *otherSubquery.whereClause) {
            return false;
        }
        for (auto i = 0u; i < patternElements.size(); ++i) {
            if (*patternElements[i] != *otherSubquery.patternElements[i]) {
                return false;
            }
        }
        return true;
    }

private:
    vector<unique_ptr<PatternElement>> patternElements;
    unique_ptr<ParsedExpression> whereClause;
};

} // namespace parser
} // namespace graphflow

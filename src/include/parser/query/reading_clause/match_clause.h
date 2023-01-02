#pragma once

#include "parser/expression/parsed_expression.h"
#include "parser/query/graph_pattern/pattern_element.h"
#include "reading_clause.h"

namespace kuzu {
namespace parser {

class MatchClause : public ReadingClause {
public:
    explicit MatchClause(
        vector<unique_ptr<PatternElement>> patternElements, bool isOptional = false)
        : ReadingClause{ClauseType::MATCH}, patternElements{move(patternElements)},
          isOptional{isOptional} {}
    ~MatchClause() = default;

    inline const vector<unique_ptr<PatternElement>>& getPatternElements() const {
        return patternElements;
    }

    inline void setWhereClause(unique_ptr<ParsedExpression> expression) {
        whereClause = move(expression);
    }

    inline bool hasWhereClause() const { return whereClause != nullptr; }
    inline ParsedExpression* getWhereClause() const { return whereClause.get(); }

    inline bool getIsOptional() const { return isOptional; }

private:
    vector<unique_ptr<PatternElement>> patternElements;
    unique_ptr<ParsedExpression> whereClause;
    bool isOptional;
};

} // namespace parser
} // namespace kuzu

#pragma once

#include "pattern_element.h"

#include "src/parser/expression/include/parsed_expression.h"

namespace graphflow {
namespace parser {

class MatchClause {

public:
    explicit MatchClause(
        vector<unique_ptr<PatternElement>> patternElements, bool isOptional = false)
        : patternElements{move(patternElements)}, isOptional{isOptional} {}

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

    bool operator==(const MatchClause& other) const;

    bool operator!=(const MatchClause& other) const { return !operator==(other); }

private:
    vector<unique_ptr<PatternElement>> patternElements;
    unique_ptr<ParsedExpression> whereClause;
    bool isOptional;
};

} // namespace parser
} // namespace graphflow

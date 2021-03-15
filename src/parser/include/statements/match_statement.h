#pragma once

#include "src/parser/include/parsed_expression.h"
#include "src/parser/include/patterns/pattern_element.h"

namespace graphflow {
namespace parser {

class MatchStatement {

public:
    explicit MatchStatement(vector<unique_ptr<PatternElement>> patternElements)
        : graphPattern{move(patternElements)} {}

    void setWhereClause(unique_ptr<ParsedExpression> whereClause) {
        this->whereClause = move(whereClause);
    }

    bool operator==(const MatchStatement& other) {
        auto result = true;
        for (auto i = 0ul; i < graphPattern.size(); ++i) {
            result &= *graphPattern[i] == *other.graphPattern[i];
        }
        if (whereClause) {
            result &= other.whereClause ? *whereClause == *other.whereClause : false;
        }
        return result;
    }

private:
    vector<unique_ptr<PatternElement>> graphPattern;
    unique_ptr<ParsedExpression> whereClause;
};

} // namespace parser
} // namespace graphflow

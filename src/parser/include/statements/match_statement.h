#pragma once

#include "src/parser/include/parsed_expression.h"
#include "src/parser/include/patterns/pattern_element.h"
#include "src/parser/include/statements/reading_statement.h"

namespace graphflow {
namespace parser {

class MatchStatement : public ReadingStatement {

public:
    explicit MatchStatement(vector<unique_ptr<PatternElement>> patternElements)
        : ReadingStatement{MATCH_STATEMENT}, graphPattern{move(patternElements)} {}

public:
    vector<unique_ptr<PatternElement>> graphPattern;
    unique_ptr<ParsedExpression> whereClause;
};

} // namespace parser
} // namespace graphflow

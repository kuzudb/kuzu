#pragma once

#include "src/parser/include/parsed_expression.h"
#include "src/parser/include/patterns/pattern_element.h"

namespace graphflow {
namespace parser {

class MatchStatement {

public:
    explicit MatchStatement(vector<unique_ptr<PatternElement>> patternElements)
        : graphPattern{move(patternElements)} {}

public:
    vector<unique_ptr<PatternElement>> graphPattern;
    unique_ptr<ParsedExpression> whereClause;
};

} // namespace parser
} // namespace graphflow

#pragma once

#include "src/parser/include/parsed_expression.h"
#include "src/parser/include/patterns/pattern_element.h"

namespace graphflow {
namespace parser {

class MatchStatement {

public:
    explicit MatchStatement(
        vector<unique_ptr<PatternElement>> patternElements, bool isOptional = false)
        : graphPattern{move(patternElements)}, isOptional{isOptional} {}

public:
    vector<unique_ptr<PatternElement>> graphPattern;
    unique_ptr<ParsedExpression> whereClause;
    bool isOptional;
};

} // namespace parser
} // namespace graphflow

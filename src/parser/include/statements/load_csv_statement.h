#pragma once

#include "src/parser/include/parsed_expression.h"
#include "src/parser/include/statements/reading_statement.h"

namespace graphflow {
namespace parser {

class LoadCSVStatement : public ReadingStatement {

public:
    LoadCSVStatement(unique_ptr<ParsedExpression> inputExpression, string lineVariableName)
        : ReadingStatement{LOAD_CSV}, inputExpression{move(inputExpression)},
          lineVariableName{move(lineVariableName)} {}

public:
    unique_ptr<ParsedExpression> inputExpression;
    string lineVariableName;
    string fieldTerminator;
};

} // namespace parser
} // namespace graphflow

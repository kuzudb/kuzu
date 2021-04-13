#pragma once

#include "src/parser/include/statements/return_statement.h"

namespace graphflow {
namespace parser {

class WithStatement : public ReturnStatement {

public:
    WithStatement(vector<unique_ptr<ParsedExpression>> expressions, bool containsStar)
        : ReturnStatement{move(expressions), containsStar} {}

public:
    unique_ptr<ParsedExpression> whereClause;
};

} // namespace parser
} // namespace graphflow

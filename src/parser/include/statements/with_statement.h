#pragma once

#include "src/parser/include/statements/return_statement.h"

namespace graphflow {
namespace parser {

/**
 * WIth statement may have where expression
 */
class WithStatement : public ReturnStatement {

public:
    WithStatement(unique_ptr<ProjectionBody> projectionBody)
        : ReturnStatement{move(projectionBody)} {}

    inline void setWhereExpression(unique_ptr<ParsedExpression> expression) {
        whereExpression = move(expression);
    }

    inline bool hasWhereExpression() const { return whereExpression != nullptr; }
    inline ParsedExpression* getWhereExpression() const { return whereExpression.get(); }

private:
    unique_ptr<ParsedExpression> whereExpression;
};

} // namespace parser
} // namespace graphflow

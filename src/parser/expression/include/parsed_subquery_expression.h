#pragma once

#include "parsed_expression.h"

#include "src/parser/query/include/single_query.h"

namespace graphflow {
namespace parser {

class ParsedSubqueryExpression : public ParsedExpression {

public:
    ParsedSubqueryExpression(ExpressionType type, unique_ptr<SingleQuery> subquery, string rawName)
        : ParsedExpression{type, move(rawName)}, subquery{move(subquery)} {}

    SingleQuery* getSubquery() const { return subquery.get(); }

private:
    unique_ptr<SingleQuery> subquery;
};

} // namespace parser
} // namespace graphflow

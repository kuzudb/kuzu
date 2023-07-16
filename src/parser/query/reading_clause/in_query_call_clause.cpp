#include "parser/query/reading_clause/in_query_call_clause.h"

namespace kuzu {
namespace parser {

std::vector<ParsedExpression*> InQueryCallClause::getParameters() const {
    std::vector<ParsedExpression*> parametersToReturn;
    parametersToReturn.reserve(parameters.size());
    for (auto& parameter : parameters) {
        parametersToReturn.push_back(parameter.get());
    }
    return parametersToReturn;
}

} // namespace parser
} // namespace kuzu

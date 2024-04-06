#include "binder/binder.h"
#include "binder/bound_explain.h"
#include "parser/explain_statement.h"

namespace kuzu {
namespace binder {

std::unique_ptr<BoundStatement> Binder::bindExplain(const parser::Statement& statement) {
    auto& explain = (parser::ExplainStatement&)statement;
    auto boundStatementToExplain = bind(*explain.getStatementToExplain());
    return std::make_unique<BoundExplain>(std::move(boundStatementToExplain),
        explain.getExplainType());
}

} // namespace binder
} // namespace kuzu

#include <memory>
#include <utility>

#include "binder/binder.h"
#include "binder/bound_explain.h"
#include "binder/bound_statement.h"
#include "parser/explain_statement.h"
#include "parser/statement.h"

namespace kuzu {
namespace binder {

std::unique_ptr<BoundStatement> Binder::bindExplain(const parser::Statement& statement) {
    auto& explainStatement = (parser::ExplainStatement&)statement;
    auto boundStatementToExplain = bind(*explainStatement.getStatementToExplain());
    return std::make_unique<BoundExplain>(
        std::move(boundStatementToExplain), explainStatement.getExplainType());
}

} // namespace binder
} // namespace kuzu

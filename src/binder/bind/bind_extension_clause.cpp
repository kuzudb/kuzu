#include "binder/binder.h"
#include "extension/extension_clause.h"

namespace kuzu {
namespace binder {

std::unique_ptr<BoundStatement> Binder::bindExtensionClause(
    const parser::Statement& statement) const {
    auto& extensionClause = statement.constCast<extension::ExtensionClause>();
    return extensionClause.getExtensionClauseHandler().bindFunc(extensionClause, *this);
}

} // namespace binder
} // namespace kuzu

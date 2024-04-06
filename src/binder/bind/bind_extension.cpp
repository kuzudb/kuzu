#include "binder/binder.h"
#include "binder/bound_extension_statement.h"
#include "common/cast.h"
#include "parser/extension_statement.h"

using namespace kuzu::parser;

namespace kuzu {
namespace binder {

std::unique_ptr<BoundStatement> Binder::bindExtension(const Statement& statement) {
    auto extensionStatement =
        common::ku_dynamic_cast<const Statement&, const ExtensionStatement&>(statement);
    return std::make_unique<BoundExtensionStatement>(extensionStatement.getAction(),
        extensionStatement.getPath());
}

} // namespace binder
} // namespace kuzu

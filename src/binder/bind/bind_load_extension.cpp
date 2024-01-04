#include "binder/binder.h"
#include "binder/bound_load_extension_statement.h"
#include "common/cast.h"
#include "parser/load_extension_statement.h"

using namespace kuzu::parser;

namespace kuzu {
namespace binder {

std::unique_ptr<BoundStatement> Binder::bindLoadExtension(const Statement& statement) {
    auto loadStatement =
        common::ku_dynamic_cast<const Statement&, const LoadExtensionStatement&>(statement);
    return std::make_unique<BoundLoadExtensionStatement>(loadStatement.getPath());
}

} // namespace binder
} // namespace kuzu

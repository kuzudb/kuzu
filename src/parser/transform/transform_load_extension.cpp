#include "parser/load_extension_statement.h"
#include "parser/transformer.h"

using namespace kuzu::common;

namespace kuzu {
namespace parser {

std::unique_ptr<Statement> Transformer::transformLoadExtension(
    CypherParser::KU_LoadExtensionContext& ctx) {
    return std::make_unique<LoadExtensionStatement>(transformStringLiteral(*ctx.StringLiteral()));
}

} // namespace parser
} // namespace kuzu

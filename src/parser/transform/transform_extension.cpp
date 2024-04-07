#include "parser/extension_statement.h"
#include "parser/transformer.h"

using namespace kuzu::common;
using namespace kuzu::extension;

namespace kuzu {
namespace parser {

std::unique_ptr<Statement> Transformer::transformExtension(CypherParser::KU_ExtensionContext& ctx) {
    if (ctx.kU_InstallExtension()) {
        return std::make_unique<ExtensionStatement>(ExtensionAction::INSTALL,
            transformVariable(*ctx.kU_InstallExtension()->oC_Variable()));
    } else {
        auto path = ctx.kU_LoadExtension()->StringLiteral() ?
                        transformStringLiteral(*ctx.kU_LoadExtension()->StringLiteral()) :
                        transformVariable(*ctx.kU_LoadExtension()->oC_Variable());
        return std::make_unique<ExtensionStatement>(ExtensionAction::LOAD, std::move(path));
    }
}

} // namespace parser
} // namespace kuzu

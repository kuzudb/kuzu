#include "parser/standalone_call.h"
#include "parser/transformer.h"

namespace kuzu {
namespace parser {

std::unique_ptr<Statement> Transformer::transformStandaloneCall(
    CypherParser::KU_StandaloneCallContext& ctx) {
    auto optionName = transformSymbolicName(*ctx.oC_SymbolicName());
    auto parameter = transformLiteral(*ctx.oC_Literal());
    return std::make_unique<StandaloneCall>(std::move(optionName), std::move(parameter));
}

} // namespace parser
} // namespace kuzu

#include "parser/standalone_call.h"
#include "parser/transformer.h"
#include "parser/void_function_call.h"

namespace kuzu {
namespace parser {

std::unique_ptr<Statement> Transformer::transformStandaloneCall(
    CypherParser::KU_StandaloneCallContext& ctx) {
    if (ctx.kU_VoidFunctionCall()) {
        return transformVoidFunctionCall(*ctx.kU_VoidFunctionCall());
    } else {
        KU_ASSERT(ctx.kU_DBSettingCall());
        return transformDBSettingCall(*ctx.kU_DBSettingCall());
    }
}

std::unique_ptr<Statement> Transformer::transformDBSettingCall(
    CypherParser::KU_DBSettingCallContext& ctx) {
    auto optionName = transformSymbolicName(*ctx.oC_SymbolicName());
    auto parameter = transformExpression(*ctx.oC_Expression());
    return std::make_unique<StandaloneCall>(std::move(optionName), std::move(parameter));
}

std::unique_ptr<Statement> Transformer::transformVoidFunctionCall(
    CypherParser::KU_VoidFunctionCallContext& ctx) {
    KU_ASSERT(ctx.oC_FunctionName());
    return std::make_unique<VoidFunctionCall>(transformFunctionName(*ctx.oC_FunctionName()));
}

} // namespace parser
} // namespace kuzu

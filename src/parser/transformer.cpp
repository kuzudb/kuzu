#include "parser/transformer.h"

#include "common/assert.h"
#include "common/string_utils.h"
#include "parser/explain_statement.h"
#include "parser/query/regular_query.h" // IWYU pragma: keep (fixes a forward declaration error)

using namespace kuzu::common;

namespace kuzu {
namespace parser {

std::unique_ptr<Statement> Transformer::transform() {
    auto statement = transformStatement(*root.oC_Statement());
    if (root.oC_AnyCypherOption()) {
        auto cypherOption = root.oC_AnyCypherOption();
        auto explainType =
            cypherOption->oC_Explain() ? ExplainType::PHYSICAL_PLAN : ExplainType::PROFILE;
        return std::make_unique<ExplainStatement>(std::move(statement), explainType);
    }
    return statement;
}

std::unique_ptr<Statement> Transformer::transformStatement(CypherParser::OC_StatementContext& ctx) {
    if (ctx.oC_Query()) {
        return transformQuery(*ctx.oC_Query());
    } else if (ctx.kU_DDL()) {
        return transformDDL(*ctx.kU_DDL());
    } else if (ctx.kU_CopyFromByColumn()) {
        return transformCopyFromByColumn(*ctx.kU_CopyFromByColumn());
    } else if (ctx.kU_CopyFrom()) {
        return transformCopyFrom(*ctx.kU_CopyFrom());
    } else if (ctx.kU_CopyTO()) {
        return transformCopyTo(*ctx.kU_CopyTO());
    } else if (ctx.kU_StandaloneCall()) {
        return transformStandaloneCall(*ctx.kU_StandaloneCall());
    } else if (ctx.kU_CreateMacro()) {
        return transformCreateMacro(*ctx.kU_CreateMacro());
    } else if (ctx.kU_CommentOn()) {
        return transformCommentOn(*ctx.kU_CommentOn());
    } else if (ctx.kU_Transaction()) {
        return transformTransaction(*ctx.kU_Transaction());
    } else {
        KU_UNREACHABLE;
    }
}

std::unique_ptr<ParsedExpression> Transformer::transformWhere(CypherParser::OC_WhereContext& ctx) {
    return transformExpression(*ctx.oC_Expression());
}

std::string Transformer::transformVariable(CypherParser::OC_VariableContext& ctx) {
    return transformSymbolicName(*ctx.oC_SymbolicName());
}

std::string Transformer::transformSchemaName(CypherParser::OC_SchemaNameContext& ctx) {
    return transformSymbolicName(*ctx.oC_SymbolicName());
}

std::string Transformer::transformSymbolicName(CypherParser::OC_SymbolicNameContext& ctx) {
    if (ctx.EscapedSymbolicName()) {
        std::string escapedSymbolName = ctx.EscapedSymbolicName()->getText();
        // escapedSymbolName symbol will be of form "`Some.Value`". Therefore, we need to sanitize
        // it such that we don't store the symbol with escape character.
        return escapedSymbolName.substr(1, escapedSymbolName.size() - 2);
    } else {
        KU_ASSERT(ctx.HexLetter() || ctx.UnescapedSymbolicName() || ctx.kU_NonReservedKeywords());
        return ctx.getText();
    }
}

std::string Transformer::transformStringLiteral(antlr4::tree::TerminalNode& stringLiteral) {
    auto str = stringLiteral.getText();
    return StringUtils::removeEscapedCharacters(str);
}

} // namespace parser
} // namespace kuzu

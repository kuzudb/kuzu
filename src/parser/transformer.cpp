#include "parser/transformer.h"

#include "common/string_utils.h"
#include "parser/explain_statement.h"
#include "parser/query/regular_query.h"

using namespace kuzu::common;

namespace kuzu {
namespace parser {

std::unique_ptr<Statement> Transformer::transform() {
    auto statement = transformOcStatement(*root.oC_Statement());
    if (root.oC_AnyCypherOption()) {
        auto cypherOption = root.oC_AnyCypherOption();
        auto explainType =
            cypherOption->oC_Explain() ? ExplainType::PHYSICAL_PLAN : ExplainType::PROFILE;
        return std::make_unique<ExplainStatement>(std::move(statement), explainType);
    }
    return statement;
}

std::unique_ptr<Statement> Transformer::transformOcStatement(
    CypherParser::OC_StatementContext& ctx) {
    if (ctx.oC_Query()) {
        return transformQuery(*ctx.oC_Query());
    } else if (ctx.kU_DDL()) {
        return transformDDL(*ctx.kU_DDL());
    } else if (ctx.kU_CopyFromNPY()) {
        return transformCopyFromNPY(*ctx.kU_CopyFromNPY());
    } else if (ctx.kU_CopyFromCSV()) {
        return transformCopyFromCSV(*ctx.kU_CopyFromCSV());
    } else if (ctx.kU_CopyTO()) {
        return transformCopyTo(*ctx.kU_CopyTO());
    } else if (ctx.kU_StandaloneCall()) {
        return transformStandaloneCall(*ctx.kU_StandaloneCall());
    } else {
        return transformCreateMacro(*ctx.kU_CreateMacro());
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
    if (ctx.UnescapedSymbolicName()) {
        return ctx.UnescapedSymbolicName()->getText();
    } else if (ctx.EscapedSymbolicName()) {
        std::string escapedSymbolName = ctx.EscapedSymbolicName()->getText();
        // escapedSymbolName symbol will be of form "`Some.Value`". Therefore, we need to sanitize
        // it such that we don't store the symbol with escape character.
        return escapedSymbolName.substr(1, escapedSymbolName.size() - 2);
    } else {
        assert(ctx.HexLetter());
        return ctx.HexLetter()->getText();
    }
}

std::string Transformer::transformStringLiteral(antlr4::tree::TerminalNode& stringLiteral) {
    auto str = stringLiteral.getText();
    return StringUtils::removeEscapedCharacters(str);
}

} // namespace parser
} // namespace kuzu

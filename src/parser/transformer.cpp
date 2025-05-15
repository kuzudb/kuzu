#include "parser/transformer.h"

#include "common/assert.h"
#include "common/string_utils.h"
#include "parser/explain_statement.h"
#include "parser/query/regular_query.h" // IWYU pragma: keep (fixes a forward declaration error)

using namespace kuzu::common;

namespace kuzu {
namespace parser {

std::vector<std::shared_ptr<Statement>> Transformer::transform() {
    std::vector<std::shared_ptr<Statement>> statements;
    for (auto& oc_Statement : root.oC_Cypher()) {
        auto statement = transformStatement(*oc_Statement->oC_Statement());
        if (oc_Statement->oC_AnyCypherOption()) {
            auto cypherOption = oc_Statement->oC_AnyCypherOption();
            auto explainType = ExplainType::PROFILE;
            if (cypherOption->oC_Explain()) {
                explainType = cypherOption->oC_Explain()->LOGICAL() ? ExplainType::LOGICAL_PLAN :
                                                                      ExplainType::PHYSICAL_PLAN;
            }
            statements.push_back(
                std::make_unique<ExplainStatement>(std::move(statement), explainType));
            continue;
        }
        statements.push_back(std::move(statement));
    }
    return statements;
}

std::unique_ptr<Statement> Transformer::transformStatement(CypherParser::OC_StatementContext& ctx) {
    if (ctx.oC_Query()) {
        return transformQuery(*ctx.oC_Query());
    } else if (ctx.kU_CreateNodeTable()) {
        return transformCreateNodeTable(*ctx.kU_CreateNodeTable());
    } else if (ctx.kU_CreateRelTable()) {
        return transformCreateRelGroup(*ctx.kU_CreateRelTable());
    } else if (ctx.kU_CreateSequence()) {
        return transformCreateSequence(*ctx.kU_CreateSequence());
    } else if (ctx.kU_CreateType()) {
        return transformCreateType(*ctx.kU_CreateType());
    } else if (ctx.kU_Drop()) {
        return transformDrop(*ctx.kU_Drop());
    } else if (ctx.kU_AlterTable()) {
        return transformAlterTable(*ctx.kU_AlterTable());
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
    } else if (ctx.kU_Extension()) {
        return transformExtension(*ctx.kU_Extension());
    } else if (ctx.kU_ExportDatabase()) {
        return transformExportDatabase(*ctx.kU_ExportDatabase());
    } else if (ctx.kU_ImportDatabase()) {
        return transformImportDatabase(*ctx.kU_ImportDatabase());
    } else if (ctx.kU_AttachDatabase()) {
        return transformAttachDatabase(*ctx.kU_AttachDatabase());
    } else if (ctx.kU_DetachDatabase()) {
        return transformDetachDatabase(*ctx.kU_DetachDatabase());
    } else if (ctx.kU_UseDatabase()) {
        return transformUseDatabase(*ctx.kU_UseDatabase());
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

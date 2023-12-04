#include "common/assert.h"
#include "parser/query/reading_clause/in_query_call_clause.h"
#include "parser/query/reading_clause/load_from.h"
#include "parser/query/reading_clause/match_clause.h"
#include "parser/query/reading_clause/unwind_clause.h"
#include "parser/transformer.h"

using namespace kuzu::common;

namespace kuzu {
namespace parser {

std::unique_ptr<ReadingClause> Transformer::transformReadingClause(
    CypherParser::OC_ReadingClauseContext& ctx) {
    if (ctx.oC_Match()) {
        return transformMatch(*ctx.oC_Match());
    } else if (ctx.oC_Unwind()) {
        return transformUnwind(*ctx.oC_Unwind());
    } else if (ctx.kU_InQueryCall()) {
        return transformInQueryCall(*ctx.kU_InQueryCall());
    } else if (ctx.kU_LoadFrom()) {
        return transformLoadFrom(*ctx.kU_LoadFrom());
    }
    KU_UNREACHABLE;
}

std::unique_ptr<ReadingClause> Transformer::transformMatch(CypherParser::OC_MatchContext& ctx) {
    auto matchClauseType =
        ctx.OPTIONAL() ? MatchClauseType::OPTIONAL_MATCH : MatchClauseType::MATCH;
    auto matchClause =
        std::make_unique<MatchClause>(transformPattern(*ctx.oC_Pattern()), matchClauseType);
    if (ctx.oC_Where()) {
        matchClause->setWherePredicate(transformWhere(*ctx.oC_Where()));
    }
    return matchClause;
}

std::unique_ptr<ReadingClause> Transformer::transformUnwind(CypherParser::OC_UnwindContext& ctx) {
    auto expression = transformExpression(*ctx.oC_Expression());
    auto transformedVariable = transformVariable(*ctx.oC_Variable());
    return std::make_unique<UnwindClause>(std::move(expression), std::move(transformedVariable));
}

std::unique_ptr<ReadingClause> Transformer::transformInQueryCall(
    CypherParser::KU_InQueryCallContext& ctx) {
    auto inQueryCall = std::make_unique<InQueryCallClause>(
        Transformer::transformFunctionInvocation(*ctx.oC_FunctionInvocation()));
    if (ctx.oC_Where()) {
        inQueryCall->setWherePredicate(transformWhere(*ctx.oC_Where()));
    }
    return inQueryCall;
}

std::unique_ptr<ReadingClause> Transformer::transformLoadFrom(
    CypherParser::KU_LoadFromContext& ctx) {
    auto filePaths = transformFilePaths(ctx.kU_FilePaths()->StringLiteral());
    auto loadFrom = std::make_unique<LoadFrom>(std::move(filePaths));
    if (ctx.kU_PropertyDefinitions()) {
        loadFrom->setColumnNameDataTypes(
            transformPropertyDefinitions(*ctx.kU_PropertyDefinitions()));
    }
    if (ctx.kU_ParsingOptions()) {
        loadFrom->setParingOptions(transformParsingOptions(*ctx.kU_ParsingOptions()));
    }
    if (ctx.oC_Where()) {
        loadFrom->setWherePredicate(transformWhere(*ctx.oC_Where()));
    }
    return loadFrom;
}

} // namespace parser
} // namespace kuzu

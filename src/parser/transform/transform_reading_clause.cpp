#include "common/exception/not_implemented.h"
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
    // LCOV_EXCL_START
    throw common::NotImplementedException("Transformer::transformReadingClause");
    // LCOV_EXCL_STOP
}

std::unique_ptr<ReadingClause> Transformer::transformMatch(CypherParser::OC_MatchContext& ctx) {
    auto matchClauseType =
        ctx.OPTIONAL() ? MatchClauseType::OPTIONAL_MATCH : MatchClauseType::MATCH;
    auto matchClause =
        std::make_unique<MatchClause>(transformPattern(*ctx.oC_Pattern()), matchClauseType);
    if (ctx.oC_Where()) {
        matchClause->setWhereClause(transformWhere(*ctx.oC_Where()));
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
    auto funcName = transformFunctionName(*ctx.oC_FunctionName());
    std::vector<std::unique_ptr<ParsedExpression>> parameter;
    for (auto& literal : ctx.oC_Literal()) {
        parameter.push_back(transformLiteral(*literal));
    }
    return std::make_unique<InQueryCallClause>(std::move(funcName), std::move(parameter));
}

std::unique_ptr<ReadingClause> Transformer::transformLoadFrom(
    CypherParser::KU_LoadFromContext& ctx) {
    auto filePaths = transformFilePaths(ctx.kU_FilePaths()->StringLiteral());
    std::unordered_map<std::string, std::unique_ptr<ParsedExpression>> parsingOptions;
    if (ctx.kU_ParsingOptions()) {
        parsingOptions = transformParsingOptions(*ctx.kU_ParsingOptions());
    }
    return std::make_unique<LoadFrom>(std::move(filePaths), std::move(parsingOptions));
}

} // namespace parser
} // namespace kuzu

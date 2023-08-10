#include "parser/query/reading_clause/in_query_call_clause.h"
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
    } else {
        assert(ctx.kU_InQueryCall());
        return transformInQueryCall(*ctx.kU_InQueryCall());
    }
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

} // namespace parser
} // namespace kuzu

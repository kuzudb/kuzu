#pragma once

#include <memory>
#include <string>

#include "src/antlr4/CypherBaseVisitor.h"
#include "src/parser/include/single_query.h"

using namespace std;

namespace graphflow {
namespace parser {

class Transformer {
public:
    unique_ptr<SingleQuery> transformParseTree(CypherParser::OC_CypherContext* ctx);

private:
    unique_ptr<SingleQuery> transformQuery(CypherParser::OC_QueryContext* ctx);

    unique_ptr<SingleQuery> transformRegularQuery(CypherParser::OC_RegularQueryContext* ctx);

    unique_ptr<SingleQuery> transformSingleQuery(CypherParser::OC_SingleQueryContext* ctx);

    unique_ptr<SingleQuery> transformSinglePartQuery(CypherParser::OC_SinglePartQueryContext* ctx);

    unique_ptr<MatchStatement> transformReadingClause(CypherParser::OC_ReadingClauseContext* ctx);

    unique_ptr<MatchStatement> transformMatch(CypherParser::OC_MatchContext* ctx);

    vector<unique_ptr<PatternElement>> transformPattern(CypherParser::OC_PatternContext* ctx);

    unique_ptr<PatternElement> transformPatternPart(CypherParser::OC_PatternPartContext* ctx);

    unique_ptr<PatternElement> transformAnonymousPatternPart(
        CypherParser::OC_AnonymousPatternPartContext* ctx);

    unique_ptr<PatternElement> transformPatternElement(CypherParser::OC_PatternElementContext* ctx);

    unique_ptr<NodePattern> transformNodePattern(CypherParser::OC_NodePatternContext* ctx);

    unique_ptr<PatternElementChain> transformPatternElementChain(
        CypherParser::OC_PatternElementChainContext* ctx);

    unique_ptr<RelPattern> transformRelationshipPattern(
        CypherParser::OC_RelationshipPatternContext* ctx);

    unique_ptr<RelPattern> transformRelationshipDetail(
        CypherParser::OC_RelationshipDetailContext* ctx);

    string transformNodeLabel(CypherParser::OC_NodeLabelContext* ctx);

    string transformLabelName(CypherParser::OC_LabelNameContext* ctx);

    string transformRelTypeName(CypherParser::OC_RelTypeNameContext* ctx);

    string transformVariable(CypherParser::OC_VariableContext* ctx);

    string transformSchemaName(CypherParser::OC_SchemaNameContext* ctx);

    string transformSymbolicName(CypherParser::OC_SymbolicNameContext* ctx);
};

} // namespace parser
} // namespace graphflow

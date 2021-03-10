#include "src/parser/include/transformer.h"

namespace graphflow {
namespace parser {

unique_ptr<SingleQuery> Transformer::transformParseTree(CypherParser::OC_CypherContext* ctx) {
    return transformQuery(ctx->oC_Statement()->oC_Query());
}

unique_ptr<SingleQuery> Transformer::transformQuery(CypherParser::OC_QueryContext* ctx) {
    return transformRegularQuery(ctx->oC_RegularQuery());
}

unique_ptr<SingleQuery> Transformer::transformRegularQuery(
    CypherParser::OC_RegularQueryContext* ctx) {
    return transformSingleQuery(ctx->oC_SingleQuery());
}

unique_ptr<SingleQuery> Transformer::transformSingleQuery(
    CypherParser::OC_SingleQueryContext* ctx) {
    return transformSinglePartQuery(ctx->oC_SinglePartQuery());
}

unique_ptr<SingleQuery> Transformer::transformSinglePartQuery(
    CypherParser::OC_SinglePartQueryContext* ctx) {
    auto singleQuery = make_unique<SingleQuery>();
    auto readingClauses = ctx->oC_ReadingClause();
    for (auto it = begin(readingClauses); it != end(readingClauses); ++it) {
        singleQuery->addMatchStatement(move(transformReadingClause(*it)));
    }
    return singleQuery;
}

unique_ptr<MatchStatement> Transformer::transformReadingClause(
    CypherParser::OC_ReadingClauseContext* ctx) {
    return transformMatch(ctx->oC_Match());
}

unique_ptr<MatchStatement> Transformer::transformMatch(CypherParser::OC_MatchContext* ctx) {
    return make_unique<MatchStatement>(move(transformPattern(ctx->oC_Pattern())));
}

vector<unique_ptr<PatternElement>> Transformer::transformPattern(
    CypherParser::OC_PatternContext* ctx) {
    vector<unique_ptr<PatternElement>> pattern;
    auto patternPartContext = ctx->oC_PatternPart();
    for (auto it = begin(patternPartContext); it != end(patternPartContext); ++it) {
        pattern.push_back(move(transformPatternPart(*it)));
    }
    return pattern;
}

unique_ptr<PatternElement> Transformer::transformPatternPart(
    CypherParser::OC_PatternPartContext* ctx) {
    return transformAnonymousPatternPart(ctx->oC_AnonymousPatternPart());
}

unique_ptr<PatternElement> Transformer::transformAnonymousPatternPart(
    CypherParser::OC_AnonymousPatternPartContext* ctx) {
    return transformPatternElement(ctx->oC_PatternElement());
}

unique_ptr<PatternElement> Transformer::transformPatternElement(
    CypherParser::OC_PatternElementContext* ctx) {
    if (ctx->oC_PatternElement()) {
        return transformPatternElement(ctx->oC_PatternElement());
    }
    auto patternElement = make_unique<PatternElement>();
    patternElement->setNodePattern(move(transformNodePattern(ctx->oC_NodePattern())));
    if (!ctx->oC_PatternElementChain().empty()) {
        auto patternElementChainContext = ctx->oC_PatternElementChain();
        for (auto it = begin(patternElementChainContext); it != end(patternElementChainContext);
             ++it) {
            patternElement->addPatternElementChain(move(transformPatternElementChain(*it)));
        }
    }
    return patternElement;
}

unique_ptr<NodePattern> Transformer::transformNodePattern(
    CypherParser::OC_NodePatternContext* ctx) {
    auto nodePattern = make_unique<NodePattern>();
    if (ctx->oC_Variable()) {
        nodePattern->setName(transformSymbolicName(ctx->oC_Variable()->oC_SymbolicName()));
    }
    if (ctx->oC_NodeLabel()) {
        nodePattern->setLabel(transformNodeLabel(ctx->oC_NodeLabel()));
    }
    return nodePattern;
}

unique_ptr<PatternElementChain> Transformer::transformPatternElementChain(
    CypherParser::OC_PatternElementChainContext* ctx) {
    auto patternElementChain = make_unique<PatternElementChain>();
    patternElementChain->setRelPattern(
        move(transformRelationshipPattern(ctx->oC_RelationshipPattern())));
    patternElementChain->setNodePattern(move(transformNodePattern(ctx->oC_NodePattern())));
    return patternElementChain;
}

unique_ptr<RelPattern> Transformer::transformRelationshipPattern(
    CypherParser::OC_RelationshipPatternContext* ctx) {
    auto relPattern = ctx->oC_RelationshipDetail() ?
                          transformRelationshipDetail(ctx->oC_RelationshipDetail()) :
                          make_unique<RelPattern>();
    relPattern->setDirection(ctx->oC_LeftArrowHead() ? BWD : FWD);
    return relPattern;
}

unique_ptr<RelPattern> Transformer::transformRelationshipDetail(
    CypherParser::OC_RelationshipDetailContext* ctx) {
    auto relPattern = make_unique<RelPattern>();
    if (ctx->oC_Variable()) {
        relPattern->setName(transformVariable(ctx->oC_Variable()));
    }
    if (ctx->oC_RelTypeName()) {
        relPattern->setType(transformRelTypeName(ctx->oC_RelTypeName()));
    }
    return relPattern;
}

string Transformer::transformNodeLabel(CypherParser::OC_NodeLabelContext* ctx) {
    return transformLabelName(ctx->oC_LabelName());
}

string Transformer::transformLabelName(CypherParser::OC_LabelNameContext* ctx) {
    return transformSchemaName(ctx->oC_SchemaName());
}

string Transformer::transformRelTypeName(CypherParser::OC_RelTypeNameContext* ctx) {
    return transformSchemaName(ctx->oC_SchemaName());
}

string Transformer::transformVariable(CypherParser::OC_VariableContext* ctx) {
    return transformSymbolicName(ctx->oC_SymbolicName());
}

string Transformer::transformSchemaName(CypherParser::OC_SchemaNameContext* ctx) {
    return transformSymbolicName(ctx->oC_SymbolicName());
}

string Transformer::transformSymbolicName(CypherParser::OC_SymbolicNameContext* ctx) {
    return ctx->UnescapedSymbolicName() ? ctx->UnescapedSymbolicName()->getText() :
                                          ctx->EscapedSymbolicName()->getText();
}

} // namespace parser
} // namespace graphflow

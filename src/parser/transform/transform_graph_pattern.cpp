#include "parser/query/graph_pattern/pattern_element.h"
#include "parser/transformer.h"

using namespace kuzu::common;

namespace kuzu {
namespace parser {

std::vector<std::unique_ptr<PatternElement>> Transformer::transformPattern(
    CypherParser::OC_PatternContext& ctx) {
    std::vector<std::unique_ptr<PatternElement>> pattern;
    for (auto& patternPart : ctx.oC_PatternPart()) {
        pattern.push_back(transformPatternPart(*patternPart));
    }
    return pattern;
}

std::unique_ptr<PatternElement> Transformer::transformPatternPart(
    CypherParser::OC_PatternPartContext& ctx) {
    auto patternElement = transformAnonymousPatternPart(*ctx.oC_AnonymousPatternPart());
    if (ctx.oC_Variable()) {
        auto variable = transformVariable(*ctx.oC_Variable());
        patternElement->setPathName(variable);
    }
    return patternElement;
}

std::unique_ptr<PatternElement> Transformer::transformAnonymousPatternPart(
    CypherParser::OC_AnonymousPatternPartContext& ctx) {
    return transformPatternElement(*ctx.oC_PatternElement());
}

std::unique_ptr<PatternElement> Transformer::transformPatternElement(
    CypherParser::OC_PatternElementContext& ctx) {
    if (ctx.oC_PatternElement()) { // parenthesized pattern element
        return transformPatternElement(*ctx.oC_PatternElement());
    }
    auto patternElement =
        std::make_unique<PatternElement>(transformNodePattern(*ctx.oC_NodePattern()));
    if (!ctx.oC_PatternElementChain().empty()) {
        for (auto& patternElementChain : ctx.oC_PatternElementChain()) {
            patternElement->addPatternElementChain(
                transformPatternElementChain(*patternElementChain));
        }
    }
    return patternElement;
}

std::unique_ptr<NodePattern> Transformer::transformNodePattern(
    CypherParser::OC_NodePatternContext& ctx) {
    auto variable = std::string();
    if (ctx.oC_Variable()) {
        variable = transformVariable(*ctx.oC_Variable());
    }
    auto nodeLabels = std::vector<std::string>{};
    if (ctx.oC_NodeLabels()) {
        nodeLabels = transformNodeLabels(*ctx.oC_NodeLabels());
    }
    auto properties = std::vector<std::pair<std::string, std::unique_ptr<ParsedExpression>>>{};
    if (ctx.kU_Properties()) {
        properties = transformProperties(*ctx.kU_Properties());
    }
    return std::make_unique<NodePattern>(
        std::move(variable), std::move(nodeLabels), std::move(properties));
}

std::unique_ptr<PatternElementChain> Transformer::transformPatternElementChain(
    CypherParser::OC_PatternElementChainContext& ctx) {
    return std::make_unique<PatternElementChain>(
        transformRelationshipPattern(*ctx.oC_RelationshipPattern()),
        transformNodePattern(*ctx.oC_NodePattern()));
}

std::unique_ptr<RelPattern> Transformer::transformRelationshipPattern(
    CypherParser::OC_RelationshipPatternContext& ctx) {
    auto relDetail = ctx.oC_RelationshipDetail();
    auto variable = std::string();
    if (relDetail->oC_Variable()) {
        variable = transformVariable(*relDetail->oC_Variable());
    }
    auto relTypes = std::vector<std::string>{};
    if (relDetail->oC_RelationshipTypes()) {
        relTypes = transformRelTypes(*relDetail->oC_RelationshipTypes());
    }
    auto properties = std::vector<std::pair<std::string, std::unique_ptr<ParsedExpression>>>{};
    if (relDetail->kU_Properties()) {
        properties = transformProperties(*relDetail->kU_Properties());
    }
    ArrowDirection arrowDirection;
    if (ctx.oC_LeftArrowHead()) {
        arrowDirection = ArrowDirection::LEFT;
    } else if (ctx.oC_RightArrowHead()) {
        arrowDirection = ArrowDirection::RIGHT;
    } else {
        arrowDirection = ArrowDirection::BOTH;
    }
    auto relType = QueryRelType::NON_RECURSIVE;
    std::unique_ptr<RecursiveRelPatternInfo> recursiveInfo;
    if (relDetail->oC_RangeLiteral()) {
        if (relDetail->oC_RangeLiteral()->ALL()) {
            relType = QueryRelType::ALL_SHORTEST;
        } else if (relDetail->oC_RangeLiteral()->SHORTEST()) {
            relType = QueryRelType::SHORTEST;
        } else {
            relType = QueryRelType::VARIABLE_LENGTH;
        }
        auto range = relDetail->oC_RangeLiteral();
        auto lowerBound = range->oC_IntegerLiteral()[0]->getText();
        auto upperBound = range->oC_IntegerLiteral()[1]->getText();
        auto recursiveRelName = std::string();
        std::unique_ptr<ParsedExpression> whereExpression = nullptr;
        if (range->oC_Where()) {
            recursiveRelName = transformVariable(*range->oC_Variable());
            whereExpression = transformWhere(*range->oC_Where());
        }
        recursiveInfo = std::make_unique<RecursiveRelPatternInfo>(
            lowerBound, upperBound, recursiveRelName, std::move(whereExpression));
    }
    return std::make_unique<RelPattern>(variable, relTypes, relType, arrowDirection,
        std::move(properties), std::move(recursiveInfo));
}

std::vector<std::pair<std::string, std::unique_ptr<ParsedExpression>>>
Transformer::transformProperties(CypherParser::KU_PropertiesContext& ctx) {
    std::vector<std::pair<std::string, std::unique_ptr<ParsedExpression>>> result;
    assert(ctx.oC_PropertyKeyName().size() == ctx.oC_Expression().size());
    for (auto i = 0u; i < ctx.oC_PropertyKeyName().size(); ++i) {
        auto propertyKeyName = transformPropertyKeyName(*ctx.oC_PropertyKeyName(i));
        auto expression = transformExpression(*ctx.oC_Expression(i));
        result.emplace_back(propertyKeyName, std::move(expression));
    }
    return result;
}

std::vector<std::string> Transformer::transformRelTypes(
    CypherParser::OC_RelationshipTypesContext& ctx) {
    std::vector<std::string> relTypes;
    for (auto& relType : ctx.oC_RelTypeName()) {
        relTypes.push_back(transformRelTypeName(*relType));
    }
    return relTypes;
}

std::vector<std::string> Transformer::transformNodeLabels(CypherParser::OC_NodeLabelsContext& ctx) {
    std::vector<std::string> nodeLabels;
    for (auto& nodeLabel : ctx.oC_NodeLabel()) {
        nodeLabels.push_back(transformNodeLabel(*nodeLabel));
    }
    return nodeLabels;
}

std::string Transformer::transformNodeLabel(CypherParser::OC_NodeLabelContext& ctx) {
    return transformLabelName(*ctx.oC_LabelName());
}

std::string Transformer::transformLabelName(CypherParser::OC_LabelNameContext& ctx) {
    return transformSchemaName(*ctx.oC_SchemaName());
}

std::string Transformer::transformRelTypeName(CypherParser::OC_RelTypeNameContext& ctx) {
    return transformSchemaName(*ctx.oC_SchemaName());
}

} // namespace parser
} // namespace kuzu

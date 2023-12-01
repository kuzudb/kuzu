#include "parser/expression/parsed_case_expression.h"

#include "common/serializer/deserializer.h"
#include "common/serializer/serializer.h"

using namespace kuzu::common;

namespace kuzu {
namespace parser {

void ParsedCaseAlternative::serialize(Serializer& serializer) const {
    whenExpression->serialize(serializer);
    thenExpression->serialize(serializer);
}

std::unique_ptr<ParsedCaseAlternative> ParsedCaseAlternative::deserialize(
    Deserializer& deserializer) {
    auto whenExpression = ParsedExpression::deserialize(deserializer);
    auto thenExpression = ParsedExpression::deserialize(deserializer);
    return std::make_unique<ParsedCaseAlternative>(
        std::move(whenExpression), std::move(thenExpression));
}

std::unique_ptr<ParsedCaseExpression> ParsedCaseExpression::deserialize(
    Deserializer& deserializer) {
    std::unique_ptr<ParsedExpression> caseExpression;
    deserializer.deserializeOptionalValue(caseExpression);
    std::vector<std::unique_ptr<ParsedCaseAlternative>> caseAlternatives;
    deserializer.deserializeVectorOfPtrs(caseAlternatives);
    std::unique_ptr<ParsedExpression> elseExpression;
    deserializer.deserializeOptionalValue(elseExpression);
    return std::make_unique<ParsedCaseExpression>(
        std::move(caseExpression), std::move(caseAlternatives), std::move(elseExpression));
}

std::unique_ptr<ParsedExpression> ParsedCaseExpression::copy() const {
    std::vector<std::unique_ptr<ParsedCaseAlternative>> caseAlternativesCopy;
    caseAlternativesCopy.reserve(caseAlternatives.size());
    for (auto& caseAlternative : caseAlternatives) {
        caseAlternativesCopy.push_back(caseAlternative->copy());
    }
    return std::make_unique<ParsedCaseExpression>(alias, rawName, copyChildren(),
        caseExpression ? caseExpression->copy() : nullptr, std::move(caseAlternativesCopy),
        elseExpression ? elseExpression->copy() : nullptr);
}

void ParsedCaseExpression::serializeInternal(Serializer& serializer) const {
    serializer.serializeOptionalValue(caseExpression);
    serializer.serializeVectorOfPtrs(caseAlternatives);
    serializer.serializeOptionalValue(elseExpression);
}

} // namespace parser
} // namespace kuzu

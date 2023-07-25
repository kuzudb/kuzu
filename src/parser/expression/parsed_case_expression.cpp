#include "parser/expression/parsed_case_expression.h"

#include "common/ser_deser.h"

namespace kuzu {
namespace parser {

void ParsedCaseAlternative::serialize(common::FileInfo* fileInfo, uint64_t& offset) const {
    whenExpression->serialize(fileInfo, offset);
    thenExpression->serialize(fileInfo, offset);
}

std::unique_ptr<ParsedCaseAlternative> ParsedCaseAlternative::deserialize(
    common::FileInfo* fileInfo, uint64_t& offset) {
    auto whenExpression = ParsedExpression::deserialize(fileInfo, offset);
    auto thenExpression = ParsedExpression::deserialize(fileInfo, offset);
    return std::make_unique<ParsedCaseAlternative>(
        std::move(whenExpression), std::move(thenExpression));
}

std::unique_ptr<ParsedCaseExpression> ParsedCaseExpression::deserialize(
    common::FileInfo* fileInfo, uint64_t& offset) {
    std::unique_ptr<ParsedExpression> caseExpression;
    common::SerDeser::deserializeOptionalValue(caseExpression, fileInfo, offset);
    std::vector<std::unique_ptr<ParsedCaseAlternative>> caseAlternatives;
    common::SerDeser::deserializeVectorOfPtrs(caseAlternatives, fileInfo, offset);
    std::unique_ptr<ParsedExpression> elseExpression;
    common::SerDeser::deserializeOptionalValue(elseExpression, fileInfo, offset);
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

void ParsedCaseExpression::serializeInternal(common::FileInfo* fileInfo, uint64_t& offset) const {
    common::SerDeser::serializeOptionalValue(caseExpression, fileInfo, offset);
    common::SerDeser::serializeVectorOfPtrs(caseAlternatives, fileInfo, offset);
    common::SerDeser::serializeOptionalValue(elseExpression, fileInfo, offset);
}

} // namespace parser
} // namespace kuzu

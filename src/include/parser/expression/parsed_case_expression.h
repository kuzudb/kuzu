#pragma once

#include "parsed_expression.h"

namespace kuzu {
namespace parser {

struct ParsedCaseAlternative {
    std::unique_ptr<ParsedExpression> whenExpression;
    std::unique_ptr<ParsedExpression> thenExpression;

    ParsedCaseAlternative(std::unique_ptr<ParsedExpression> whenExpression,
        std::unique_ptr<ParsedExpression> thenExpression)
        : whenExpression{std::move(whenExpression)}, thenExpression{std::move(thenExpression)} {}

    void serialize(common::Serializer& serializer) const;
    static std::unique_ptr<ParsedCaseAlternative> deserialize(common::Deserializer& deserializer);

    inline std::unique_ptr<ParsedCaseAlternative> copy() const {
        return std::make_unique<ParsedCaseAlternative>(
            whenExpression->copy(), thenExpression->copy());
    }
};

// Cypher supports 2 types of CaseExpression
// 1. CASE a.age
//    WHEN 20 THEN ...
// 2. CASE
//    WHEN a.age = 20 THEN ...
class ParsedCaseExpression : public ParsedExpression {
    friend class ParsedExpressionChildrenVisitor;

public:
    explicit ParsedCaseExpression(std::string raw)
        : ParsedExpression{common::ExpressionType::CASE_ELSE, std::move(raw)} {};

    ParsedCaseExpression(std::string alias, std::string rawName, parsed_expression_vector children,
        std::unique_ptr<ParsedExpression> caseExpression,
        std::vector<std::unique_ptr<ParsedCaseAlternative>> caseAlternatives,
        std::unique_ptr<ParsedExpression> elseExpression)
        : ParsedExpression{common::ExpressionType::CASE_ELSE, std::move(alias), std::move(rawName),
              std::move(children)},
          caseExpression{std::move(caseExpression)}, caseAlternatives{std::move(caseAlternatives)},
          elseExpression{std::move(elseExpression)} {}

    ParsedCaseExpression(std::unique_ptr<ParsedExpression> caseExpression,
        std::vector<std::unique_ptr<ParsedCaseAlternative>> caseAlternatives,
        std::unique_ptr<ParsedExpression> elseExpression)
        : ParsedExpression{common::ExpressionType::CASE_ELSE}, caseExpression{std::move(
                                                                   caseExpression)},
          caseAlternatives{std::move(caseAlternatives)}, elseExpression{std::move(elseExpression)} {
    }

    inline void setCaseExpression(std::unique_ptr<ParsedExpression> expression) {
        caseExpression = std::move(expression);
    }
    inline bool hasCaseExpression() const { return caseExpression != nullptr; }
    inline ParsedExpression* getCaseExpression() const { return caseExpression.get(); }

    inline void addCaseAlternative(std::unique_ptr<ParsedCaseAlternative> caseAlternative) {
        caseAlternatives.push_back(std::move(caseAlternative));
    }
    inline uint32_t getNumCaseAlternative() const { return caseAlternatives.size(); }
    inline ParsedCaseAlternative* getCaseAlternative(uint32_t idx) const {
        return caseAlternatives[idx].get();
    }

    inline void setElseExpression(std::unique_ptr<ParsedExpression> expression) {
        elseExpression = std::move(expression);
    }
    inline bool hasElseExpression() const { return elseExpression != nullptr; }
    inline ParsedExpression* getElseExpression() const { return elseExpression.get(); }

    static std::unique_ptr<ParsedCaseExpression> deserialize(common::Deserializer& deserializer);

    std::unique_ptr<ParsedExpression> copy() const override;

private:
    void serializeInternal(common::Serializer& serializer) const override;

private:
    // Optional. If not specified, directly check next whenExpression
    std::unique_ptr<ParsedExpression> caseExpression;
    std::vector<std::unique_ptr<ParsedCaseAlternative>> caseAlternatives;
    // Optional. If not specified, evaluate as null
    std::unique_ptr<ParsedExpression> elseExpression;
};

} // namespace parser
} // namespace kuzu

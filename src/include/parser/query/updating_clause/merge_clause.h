#pragma once

#include "parser/query/graph_pattern/pattern_element.h"
#include "updating_clause.h"

namespace kuzu {
namespace parser {

class MergeClause : public UpdatingClause {
public:
    explicit MergeClause(std::vector<std::unique_ptr<PatternElement>> patternElements)
        : UpdatingClause{common::ClauseType::MERGE}, patternElements{std::move(patternElements)} {}

    inline const std::vector<std::unique_ptr<PatternElement>>& getPatternElementsRef() const {
        return patternElements;
    }
    inline void addOnMatchSetItems(parsed_expression_pair setItem) {
        onMatchSetItems.push_back(std::move(setItem));
    }
    inline bool hasOnMatchSetItems() const { return !onMatchSetItems.empty(); }
    inline uint32_t getNumOnMatchSetItems() const { return onMatchSetItems.size(); }
    inline std::pair<ParsedExpression*, ParsedExpression*> getOnMatchSetItem(uint32_t idx) const {
        auto& [left, right] = onMatchSetItems[idx];
        return std::make_pair(left.get(), right.get());
    }

    inline void addOnCreateSetItems(parsed_expression_pair setItem) {
        onCreateSetItems.push_back(std::move(setItem));
    }
    inline bool hasOnCreateSetItems() const { return !onCreateSetItems.empty(); }
    inline uint32_t getNumOnCreateSetItems() const { return onCreateSetItems.size(); }
    inline std::pair<ParsedExpression*, ParsedExpression*> getOnCreateSetItem(uint32_t idx) const {
        auto& [left, right] = onCreateSetItems[idx];
        return std::make_pair(left.get(), right.get());
    }

private:
    std::vector<std::unique_ptr<PatternElement>> patternElements;
    std::vector<parsed_expression_pair> onMatchSetItems;
    std::vector<parsed_expression_pair> onCreateSetItems;
};

} // namespace parser
} // namespace kuzu

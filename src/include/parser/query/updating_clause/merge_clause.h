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
    inline const std::vector<parsed_expression_pair>& getOnMatchSetItemsRef() const {
        return onMatchSetItems;
    }

    inline void addOnCreateSetItems(parsed_expression_pair setItem) {
        onCreateSetItems.push_back(std::move(setItem));
    }
    inline bool hasOnCreateSetItems() const { return !onCreateSetItems.empty(); }
    inline const std::vector<parsed_expression_pair>& getOnCreateSetItemsRef() const {
        return onCreateSetItems;
    }

private:
    std::vector<std::unique_ptr<PatternElement>> patternElements;
    std::vector<parsed_expression_pair> onMatchSetItems;
    std::vector<parsed_expression_pair> onCreateSetItems;
};

} // namespace parser
} // namespace kuzu

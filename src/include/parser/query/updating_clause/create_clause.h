#pragma once

#include "parser/query/graph_pattern/pattern_element.h"
#include "updating_clause.h"

namespace kuzu {
namespace parser {

class CreateClause : public UpdatingClause {
public:
    explicit CreateClause(std::vector<std::unique_ptr<PatternElement>> patternElements)
        : UpdatingClause{common::ClauseType::CREATE}, patternElements{
                                                          std::move(patternElements)} {};

    inline const std::vector<std::unique_ptr<PatternElement>>& getPatternElementsRef() const {
        return patternElements;
    }

private:
    std::vector<std::unique_ptr<PatternElement>> patternElements;
};

} // namespace parser
} // namespace kuzu

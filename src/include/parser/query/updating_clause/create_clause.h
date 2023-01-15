#pragma once

#include "parser/query/graph_pattern/pattern_element.h"
#include "updating_clause.h"

namespace kuzu {
namespace parser {

class CreateClause : public UpdatingClause {
public:
    CreateClause(vector<unique_ptr<PatternElement>> patternElements)
        : UpdatingClause{ClauseType::CREATE}, patternElements{std::move(patternElements)} {};

    inline const vector<unique_ptr<PatternElement>>& getPatternElements() const {
        return patternElements;
    }

private:
    vector<unique_ptr<PatternElement>> patternElements;
};

} // namespace parser
} // namespace kuzu

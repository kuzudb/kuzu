#pragma once

#include "src/parser/include/patterns/pattern_element.h"

namespace graphflow {
namespace parser {

class MatchStatement {

public:
    explicit MatchStatement(vector<unique_ptr<PatternElement>> patternElements)
        : patternElements{move(patternElements)} {}

    bool operator==(const MatchStatement& other) {
        auto result = true;
        for (auto i = 0ul; i < patternElements.size(); ++i) {
            result &= *patternElements[i] == *other.patternElements[i];
        }
        return result;
    }

private:
    vector<unique_ptr<PatternElement>> patternElements;
};

} // namespace parser
} // namespace graphflow

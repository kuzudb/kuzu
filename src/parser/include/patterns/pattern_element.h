#pragma once

#include <vector>

#include "src/parser/include/patterns/pattern_element_chain.h"

namespace graphflow {
namespace parser {

/**
 * PatternElement represents "NodePattern - PatternElementChain - ..."
 */
class PatternElement {

public:
    explicit PatternElement(unique_ptr<NodePattern> nodePattern) : nodePattern{move(nodePattern)} {}

    bool operator==(const PatternElement& other) {
        auto result = *nodePattern == *other.nodePattern;
        for (auto i = 0ul; i < patternElementChains.size(); ++i) {
            result &= *patternElementChains[i] == *other.patternElementChains[i];
        }
        return result;
    }

public:
    unique_ptr<NodePattern> nodePattern;
    vector<unique_ptr<PatternElementChain>> patternElementChains;
};

} // namespace parser
} // namespace graphflow

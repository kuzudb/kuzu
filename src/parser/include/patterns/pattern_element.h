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
    const NodePattern& getNodePattern() const { return *nodePattern; }

    int getNumPatternElementChain() const { return patternElementChains.size(); }

    const PatternElementChain& getPatternElementChain(int idx) const {
        return *patternElementChains[idx];
    }

    void setNodePattern(unique_ptr<NodePattern> nodePattern) {
        this->nodePattern = move(nodePattern);
    }

    void addPatternElementChain(unique_ptr<PatternElementChain> patternElementChain) {
        patternElementChains.push_back(move(patternElementChain));
    }

    bool operator==(const PatternElement& other) {
        auto result = *nodePattern == *other.nodePattern;
        for (auto i = 0ul; i < patternElementChains.size(); ++i) {
            result &= *patternElementChains[i] == *other.patternElementChains[i];
        }
        return result;
    }

private:
    unique_ptr<NodePattern> nodePattern;
    vector<unique_ptr<PatternElementChain>> patternElementChains;
};

} // namespace parser
} // namespace graphflow

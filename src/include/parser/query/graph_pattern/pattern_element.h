#pragma once

#include <vector>

#include "pattern_element_chain.h"

namespace kuzu {
namespace parser {

/**
 * PatternElement represents "NodePattern - PatternElementChain - ..."
 */
class PatternElement {

public:
    explicit PatternElement(unique_ptr<NodePattern> nodePattern) : nodePattern{move(nodePattern)} {}

    ~PatternElement() = default;

    inline NodePattern* getFirstNodePattern() const { return nodePattern.get(); }

    inline void addPatternElementChain(unique_ptr<PatternElementChain> patternElementChain) {
        patternElementChains.push_back(move(patternElementChain));
    }

    inline uint32_t getNumPatternElementChains() const { return patternElementChains.size(); }

    inline PatternElementChain* getPatternElementChain(uint32_t idx) const {
        return patternElementChains[idx].get();
    }

private:
    unique_ptr<NodePattern> nodePattern;
    vector<unique_ptr<PatternElementChain>> patternElementChains;
};

} // namespace parser
} // namespace kuzu

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
    explicit PatternElement(std::unique_ptr<NodePattern> nodePattern)
        : nodePattern{std::move(nodePattern)} {}

    ~PatternElement() = default;

    inline void setPathName(std::string name) { pathName = std::move(name); }
    inline bool hasPathName() const { return !pathName.empty(); }
    inline std::string getPathName() const { return pathName; }

    inline NodePattern* getFirstNodePattern() const { return nodePattern.get(); }

    inline void addPatternElementChain(std::unique_ptr<PatternElementChain> patternElementChain) {
        patternElementChains.push_back(std::move(patternElementChain));
    }
    inline uint32_t getNumPatternElementChains() const { return patternElementChains.size(); }
    inline PatternElementChain* getPatternElementChain(uint32_t idx) const {
        return patternElementChains[idx].get();
    }

private:
    std::string pathName;
    std::unique_ptr<NodePattern> nodePattern;
    std::vector<std::unique_ptr<PatternElementChain>> patternElementChains;
};

} // namespace parser
} // namespace kuzu

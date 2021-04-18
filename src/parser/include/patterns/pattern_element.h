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

public:
    unique_ptr<NodePattern> nodePattern;
    vector<unique_ptr<PatternElementChain>> patternElementChains;
};

} // namespace parser
} // namespace graphflow

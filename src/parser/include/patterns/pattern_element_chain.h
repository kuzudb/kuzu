#pragma once

#include "src/parser/include/patterns/rel_pattern.h"

namespace graphflow {
namespace parser {

/**
 * PatternElementChain represents " - RelPattern - NodePattern"
 */
class PatternElementChain {

public:
    PatternElementChain(unique_ptr<RelPattern> relPattern, unique_ptr<NodePattern> nodePattern)
        : relPattern{move(relPattern)}, nodePattern{move(nodePattern)} {}

    bool operator==(const PatternElementChain& other) {
        return *relPattern == *other.relPattern && *nodePattern == *other.nodePattern;
    }

public:
    unique_ptr<RelPattern> relPattern;
    unique_ptr<NodePattern> nodePattern;
};

} // namespace parser
} // namespace graphflow

#pragma once

#include "src/parser/include/patterns/rel_pattern.h"

namespace graphflow {
namespace parser {

/**
 * PatternElementChain represents " - RelPattern - NodePattern"
 */
class PatternElementChain {

public:
    const RelPattern& getRelPattern() const { return *relPattern; }

    const NodePattern& getNodePattern() const { return *nodePattern; }

    void setRelPattern(unique_ptr<RelPattern> relPattern) { this->relPattern = move(relPattern); }

    void setNodePattern(unique_ptr<NodePattern> nodePattern) {
        this->nodePattern = move(nodePattern);
    }

    bool operator==(const PatternElementChain& other) {
        return *relPattern == *other.relPattern && *nodePattern == *other.nodePattern;
    }

private:
    unique_ptr<RelPattern> relPattern;
    unique_ptr<NodePattern> nodePattern;
};

} // namespace parser
} // namespace graphflow

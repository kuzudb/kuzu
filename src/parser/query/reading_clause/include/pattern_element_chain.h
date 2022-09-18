#pragma once

#include "rel_pattern.h"

namespace graphflow {
namespace parser {

/**
 * PatternElementChain represents " - RelPattern - NodePattern"
 */
class PatternElementChain {

public:
    PatternElementChain(unique_ptr<RelPattern> relPattern, unique_ptr<NodePattern> nodePattern)
        : relPattern{move(relPattern)}, nodePattern{move(nodePattern)} {}

    ~PatternElementChain() = default;

    inline RelPattern* getRelPattern() const { return relPattern.get(); }

    inline NodePattern* getNodePattern() const { return nodePattern.get(); }

    bool operator==(const PatternElementChain& other) const {
        return *relPattern == *other.relPattern;
    }

    bool operator!=(const PatternElementChain& other) const { return !operator==(other); }

private:
    unique_ptr<RelPattern> relPattern;
    unique_ptr<NodePattern> nodePattern;
};

} // namespace parser
} // namespace graphflow

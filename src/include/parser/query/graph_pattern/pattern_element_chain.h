#pragma once

#include "rel_pattern.h"

namespace kuzu {
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

private:
    unique_ptr<RelPattern> relPattern;
    unique_ptr<NodePattern> nodePattern;
};

} // namespace parser
} // namespace kuzu

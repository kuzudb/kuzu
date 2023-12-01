#pragma once

#include "rel_pattern.h"

namespace kuzu {
namespace parser {

/**
 * PatternElementChain represents " - RelPattern - NodePattern"
 */
class PatternElementChain {

public:
    PatternElementChain(
        std::unique_ptr<RelPattern> relPattern, std::unique_ptr<NodePattern> nodePattern)
        : relPattern{std::move(relPattern)}, nodePattern{std::move(nodePattern)} {}

    ~PatternElementChain() = default;

    inline RelPattern* getRelPattern() const { return relPattern.get(); }

    inline NodePattern* getNodePattern() const { return nodePattern.get(); }

private:
    std::unique_ptr<RelPattern> relPattern;
    std::unique_ptr<NodePattern> nodePattern;
};

} // namespace parser
} // namespace kuzu

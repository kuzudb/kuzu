#pragma once

#include "src/parser/include/patterns/node_pattern.h"

namespace graphflow {
namespace parser {

enum ArrowDirection : uint8_t { LEFT = 0, RIGHT = 1 };

/**
 * RelationshipPattern represents "-[relName:RelLabel]-"
 */
class RelPattern {

public:
    RelPattern(string name, string label, uint8_t lowerBound, uint8_t upperBound)
        : name{move(name)}, label{move(label)}, lowerBound{lowerBound}, upperBound{upperBound} {}

public:
    string name;
    string label;
    uint64_t lowerBound;
    uint64_t upperBound;
    ArrowDirection arrowDirection;
};

} // namespace parser
} // namespace graphflow

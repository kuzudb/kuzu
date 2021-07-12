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
    RelPattern(string name, string label, string lowerBound, string upperBound)
        : name{move(name)}, label{move(label)}, lowerBound{move(lowerBound)}, upperBound{move(
                                                                                  upperBound)} {}

public:
    string name;
    string label;
    string lowerBound;
    string upperBound;
    ArrowDirection arrowDirection;
};

} // namespace parser
} // namespace graphflow

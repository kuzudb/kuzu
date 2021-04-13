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
    RelPattern(string name, string label) : name{move(name)}, label{move(label)} {}

public:
    string name;
    string label;
    ArrowDirection arrowDirection;
};

} // namespace parser
} // namespace graphflow

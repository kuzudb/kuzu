#pragma once

#include "node_pattern.h"

namespace graphflow {
namespace parser {

enum ArrowDirection : uint8_t { LEFT = 0, RIGHT = 1 };

/**
 * RelationshipPattern represents "-[relName:RelLabel]-"
 */
class RelPattern {

public:
    RelPattern(string name, string label, string lowerBound, string upperBound,
        ArrowDirection arrowDirection)
        : name{move(name)}, label{move(label)}, lowerBound{move(lowerBound)},
          upperBound{move(upperBound)}, arrowDirection{arrowDirection} {}

    ~RelPattern() = default;

    inline string getName() const { return name; }

    inline string getLabel() const { return label; }

    inline string getLowerBound() const { return lowerBound; }

    inline string getUpperBound() const { return upperBound; }

    inline ArrowDirection getDirection() const { return arrowDirection; }

    bool operator==(const RelPattern& other) const {
        return name == other.name && label == other.label && lowerBound == other.lowerBound &&
               upperBound == other.upperBound && arrowDirection == other.arrowDirection;
    }

private:
    string name;
    string label;
    string lowerBound;
    string upperBound;
    ArrowDirection arrowDirection;
};

} // namespace parser
} // namespace graphflow

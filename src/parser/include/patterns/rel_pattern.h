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
    string getName() const { return name; }

    string getType() const { return type; }

    ArrowDirection getDirection() const { return arrowDirection; }

    void setName(const string& name) { this->name = name; }

    void setType(const string& type) { this->type = type; }

    void setDirection(ArrowDirection arrowDirection) { this->arrowDirection = arrowDirection; }

    bool operator==(const RelPattern& other) {
        return name == other.name && type == other.type && arrowDirection == other.arrowDirection;
    }

private:
    string name;
    string type;
    ArrowDirection arrowDirection;
};

} // namespace parser
} // namespace graphflow

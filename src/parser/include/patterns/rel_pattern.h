#pragma once

#include "src/common/include/types.h"
#include "src/parser/include/patterns/node_pattern.h"

using namespace graphflow::common;

namespace graphflow {
namespace parser {

/**
 * RelationshipPattern represents "-[relName:RelLabel]-"
 */
class RelPattern {

public:
    void setName(const string& name) { this->name = name; }

    void setType(const string& type) { this->type = type; }

    void setDirection(Direction direction) { this->direction = direction; }

    bool operator==(const RelPattern& other) {
        return name == other.name && type == other.type && direction == other.direction;
    }

private:
    string name;

    string type;

    Direction direction;
};

} // namespace parser
} // namespace graphflow

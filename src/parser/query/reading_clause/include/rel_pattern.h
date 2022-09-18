#pragma once

#include "node_pattern.h"

namespace graphflow {
namespace parser {

enum ArrowDirection : uint8_t { LEFT = 0, RIGHT = 1 };

/**
 * RelationshipPattern represents "-[relName:RelTable]-"
 */
class RelPattern {

public:
    RelPattern(string name, string tableName, string lowerBound, string upperBound,
        ArrowDirection arrowDirection)
        : variableName{move(name)}, tableName{move(tableName)}, lowerBound{move(lowerBound)},
          upperBound{move(upperBound)}, arrowDirection{arrowDirection} {}

    ~RelPattern() = default;

    inline string getVariableName() const { return variableName; }

    inline string getTableName() const { return tableName; }

    inline string getLowerBound() const { return lowerBound; }

    inline string getUpperBound() const { return upperBound; }

    inline ArrowDirection getDirection() const { return arrowDirection; }

    bool operator==(const RelPattern& other) const {
        return variableName == other.variableName && tableName == other.tableName &&
               lowerBound == other.lowerBound && upperBound == other.upperBound &&
               arrowDirection == other.arrowDirection;
    }

private:
    string variableName;
    string tableName;
    string lowerBound;
    string upperBound;
    ArrowDirection arrowDirection;
};

} // namespace parser
} // namespace graphflow

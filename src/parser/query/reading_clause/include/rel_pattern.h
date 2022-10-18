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
        ArrowDirection arrowDirection,
        vector<pair<string, unique_ptr<ParsedExpression>>> propertyKeyValPairs)
        : variableName{std::move(name)}, tableName{std::move(tableName)},
          lowerBound{std::move(lowerBound)}, upperBound{std::move(upperBound)},
          arrowDirection{arrowDirection}, propertyKeyValPairs{std::move(propertyKeyValPairs)} {}

    ~RelPattern() = default;

    inline string getVariableName() const { return variableName; }

    inline string getTableName() const { return tableName; }

    inline string getLowerBound() const { return lowerBound; }

    inline string getUpperBound() const { return upperBound; }

    inline ArrowDirection getDirection() const { return arrowDirection; }

    inline uint32_t getNumPropertyKeyValPairs() const { return propertyKeyValPairs.size(); }
    inline pair<string, ParsedExpression*> getProperty(uint32_t idx) const {
        return make_pair(propertyKeyValPairs[idx].first, propertyKeyValPairs[idx].second.get());
    }

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
    vector<pair<string, unique_ptr<ParsedExpression>>> propertyKeyValPairs;
};

} // namespace parser
} // namespace graphflow

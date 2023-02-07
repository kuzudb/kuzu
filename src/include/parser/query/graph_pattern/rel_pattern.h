#pragma once

#include "node_pattern.h"

namespace kuzu {
namespace parser {

enum ArrowDirection : uint8_t { LEFT = 0, RIGHT = 1 };

/**
 * RelationshipPattern represents "-[relName:RelTableName+]-"
 */
class RelPattern : public NodePattern {
public:
    RelPattern(std::string name, std::vector<std::string> tableNames, std::string lowerBound,
        std::string upperBound, ArrowDirection arrowDirection,
        std::vector<std::pair<std::string, std::unique_ptr<ParsedExpression>>> propertyKeyValPairs)
        : NodePattern{std::move(name), std::move(tableNames), std::move(propertyKeyValPairs)},
          lowerBound{std::move(lowerBound)}, upperBound{std::move(upperBound)},
          arrowDirection{arrowDirection} {}

    ~RelPattern() override = default;

    inline std::string getLowerBound() const { return lowerBound; }

    inline std::string getUpperBound() const { return upperBound; }

    inline ArrowDirection getDirection() const { return arrowDirection; }

private:
    std::string lowerBound;
    std::string upperBound;
    ArrowDirection arrowDirection;
};

} // namespace parser
} // namespace kuzu

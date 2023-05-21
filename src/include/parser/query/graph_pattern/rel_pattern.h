#pragma once

#include "common/query_rel_type.h"
#include "node_pattern.h"

namespace kuzu {
namespace parser {

enum class ArrowDirection : uint8_t { LEFT = 0, RIGHT = 1, BOTH = 2 };
/**
 * RelationshipPattern represents "-[relName:RelTableName+]-"
 */
class RelPattern : public NodePattern {
public:
    RelPattern(std::string name, std::vector<std::string> tableNames, common::QueryRelType relType,
        std::string lowerBound, std::string upperBound, ArrowDirection arrowDirection,
        std::vector<std::pair<std::string, std::unique_ptr<ParsedExpression>>> propertyKeyValPairs)
        : NodePattern{std::move(name), std::move(tableNames), std::move(propertyKeyValPairs)},
          relType{relType}, lowerBound{std::move(lowerBound)}, upperBound{std::move(upperBound)},
          arrowDirection{arrowDirection} {}

    ~RelPattern() override = default;

    inline common::QueryRelType getRelType() const { return relType; }

    inline std::string getLowerBound() const { return lowerBound; }

    inline std::string getUpperBound() const { return upperBound; }

    inline ArrowDirection getDirection() const { return arrowDirection; }

private:
    common::QueryRelType relType;
    std::string lowerBound;
    std::string upperBound;
    ArrowDirection arrowDirection;
};

} // namespace parser
} // namespace kuzu

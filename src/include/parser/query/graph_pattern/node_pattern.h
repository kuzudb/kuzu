#pragma once

#include <memory>
#include <string>

#include "parser/expression/parsed_expression.h"

namespace kuzu {
namespace parser {

/**
 * NodePattern represents "(nodeName:NodeTable+ {p1:v1, p2:v2, ...})"
 */
class NodePattern {
public:
    NodePattern(std::string name, std::vector<std::string> tableNames,
        std::vector<std::pair<std::string, std::unique_ptr<ParsedExpression>>> propertyKeyValPairs)
        : variableName{std::move(name)}, tableNames{std::move(tableNames)},
          propertyKeyValPairs{std::move(propertyKeyValPairs)} {}

    virtual ~NodePattern() = default;

    inline std::string getVariableName() const { return variableName; }

    inline std::vector<std::string> getTableNames() const { return tableNames; }

    inline uint32_t getNumPropertyKeyValPairs() const { return propertyKeyValPairs.size(); }
    inline std::pair<std::string, ParsedExpression*> getProperty(uint32_t idx) const {
        return std::make_pair(
            propertyKeyValPairs[idx].first, propertyKeyValPairs[idx].second.get());
    }

    void setTableNames(std::vector<std::string> otherTableNames) {
        tableNames = std::move(otherTableNames);
    }

protected:
    std::string variableName;
    std::vector<std::string> tableNames;
    std::vector<std::pair<std::string, std::unique_ptr<ParsedExpression>>> propertyKeyValPairs;
};

} // namespace parser
} // namespace kuzu

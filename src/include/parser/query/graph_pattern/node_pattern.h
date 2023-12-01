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
    using property_key_val_t = std::pair<std::string, std::unique_ptr<ParsedExpression>>;

    NodePattern(std::string name, std::vector<std::string> tableNames,
        std::vector<property_key_val_t> propertyKeyVals)
        : variableName{std::move(name)}, tableNames{std::move(tableNames)},
          propertyKeyVals{std::move(propertyKeyVals)} {}

    virtual ~NodePattern() = default;

    inline std::string getVariableName() const { return variableName; }

    inline std::vector<std::string> getTableNames() const { return tableNames; }

    inline const std::vector<property_key_val_t>& getPropertyKeyVals() const {
        return propertyKeyVals;
    }

protected:
    std::string variableName;
    std::vector<std::string> tableNames;
    std::vector<property_key_val_t> propertyKeyVals;
};

} // namespace parser
} // namespace kuzu

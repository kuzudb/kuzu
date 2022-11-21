#pragma once

#include <memory>
#include <string>

#include "parser/expression/parsed_expression.h"

using namespace std;

namespace kuzu {
namespace parser {

/**
 * NodePattern represents "(nodeName:NodeTable {p1:v1, p2:v2, ...})"
 */
class NodePattern {
public:
    NodePattern(string name, string tableName,
        vector<pair<string, unique_ptr<ParsedExpression>>> propertyKeyValPairs)
        : variableName{std::move(name)}, tableName{std::move(tableName)},
          propertyKeyValPairs{std::move(propertyKeyValPairs)} {}

    virtual ~NodePattern() = default;

    inline string getVariableName() const { return variableName; }

    inline string getTableName() const { return tableName; }

    inline uint32_t getNumPropertyKeyValPairs() const { return propertyKeyValPairs.size(); }
    inline pair<string, ParsedExpression*> getProperty(uint32_t idx) const {
        return make_pair(propertyKeyValPairs[idx].first, propertyKeyValPairs[idx].second.get());
    }

private:
    string variableName;
    string tableName;
    vector<pair<string, unique_ptr<ParsedExpression>>> propertyKeyValPairs;
};

} // namespace parser
} // namespace kuzu

#pragma once

#include <memory>
#include <string>

#include "src/parser/expression/include/parsed_expression.h"

using namespace std;

namespace graphflow {
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

    ~NodePattern() = default;

    inline string getVariableName() const { return variableName; }

    inline string getTableName() const { return tableName; }

    inline uint32_t getNumPropertyKeyValPairs() const { return propertyKeyValPairs.size(); }
    inline pair<string, ParsedExpression*> getProperty(uint32_t idx) const {
        return make_pair(propertyKeyValPairs[idx].first, propertyKeyValPairs[idx].second.get());
    }

    bool operator==(const NodePattern& other) const {
        if (!(variableName == other.variableName && tableName == other.tableName &&
                propertyKeyValPairs.size() == other.propertyKeyValPairs.size())) {
            return false;
        }
        for (auto i = 0u; i < propertyKeyValPairs.size(); ++i) {
            auto& [name, expression] = propertyKeyValPairs[i];
            auto& [otherName, otherExpression] = other.propertyKeyValPairs[i];
            if (!(name == otherName && *expression == *otherExpression)) {
                return false;
            }
        }
        return true;
    }

    bool operator!=(const NodePattern& other) const { return !operator==(other); }

private:
    string variableName;
    string tableName;
    vector<pair<string, unique_ptr<ParsedExpression>>> propertyKeyValPairs;
};

} // namespace parser
} // namespace graphflow

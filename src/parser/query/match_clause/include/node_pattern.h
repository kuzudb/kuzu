#pragma once

#include <memory>
#include <string>

#include "src/parser/expression/include/parsed_expression.h"

using namespace std;

namespace graphflow {
namespace parser {

/**
 * NodePattern represents "(nodeName:NodeLabel {p1:v1, p2:v2, ...})"
 */
class NodePattern {
public:
    NodePattern(
        string name, string label, vector<pair<string, unique_ptr<ParsedExpression>>> properties)
        : name{move(name)}, label{move(label)}, properties{move(properties)} {}

    ~NodePattern() = default;

    inline string getName() const { return name; }

    inline string getLabel() const { return label; }

    inline uint32_t getNumProperties() const { return properties.size(); }
    inline pair<string, ParsedExpression*> getProperty(uint32_t idx) const {
        return make_pair(properties[idx].first, properties[idx].second.get());
    }

    bool operator==(const NodePattern& other) const {
        if (!(name == other.name && label == other.label &&
                properties.size() == other.properties.size())) {
            return false;
        }
        for (auto i = 0u; i < properties.size(); ++i) {
            auto& [name, expression] = properties[i];
            auto& [otherName, otherExpression] = other.properties[i];
            if (!(name == otherName && *expression == *otherExpression)) {
                return false;
            }
        }
        return true;
    }

    bool operator!=(const NodePattern& other) const { return !operator==(other); }

private:
    string name;
    string label;
    vector<pair<string, unique_ptr<ParsedExpression>>> properties;
};

} // namespace parser
} // namespace graphflow

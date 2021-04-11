#pragma once

#include <unordered_map>

#include "src/planner/include/logical_plan/operator/logical_operator.h"

namespace graphflow {
namespace planner {

class Schema {

public:
    Schema() = default;

    explicit Schema(unordered_map<string, LogicalOperator*> nameOperatorMap)
        : nameOperatorMap{move(nameOperatorMap)} {}

    void addOperator(const string& name, LogicalOperator* op) {
        nameOperatorMap.insert({name, op});
    }

    bool containsName(const string& name) {
        return end(nameOperatorMap) != nameOperatorMap.find(name);
    }

    LogicalOperator* getOperator(const string& name) { return nameOperatorMap.at(name); }

    unique_ptr<Schema> copy() { return make_unique<Schema>(nameOperatorMap); }

public:
    unordered_map<string, LogicalOperator*> nameOperatorMap;
};

} // namespace planner
} // namespace graphflow

#pragma once

#include "src/planner/include/logical_plan/operator/logical_operator.h"

namespace graphflow {
namespace planner {

class Schema {

public:
    void addOperator(const string& varName, LogicalOperator* logicalOperator) {
        varNameOperatorMap.insert({varName, logicalOperator});
    }

    bool containsOperator(const string& varName) {
        return end(varNameOperatorMap) != varNameOperatorMap.find(varName);
    }

    LogicalOperator* getOperator(const string& varName) { return varNameOperatorMap.at(varName); }

    unique_ptr<Schema> copy() {
        auto newSchema = make_unique<Schema>();
        newSchema->varNameOperatorMap = varNameOperatorMap;
        return newSchema;
    }

public:
    unordered_map<string, LogicalOperator*> varNameOperatorMap;
};

} // namespace planner
} // namespace graphflow

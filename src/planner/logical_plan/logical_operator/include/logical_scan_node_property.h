#pragma once

#include "base_logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalScanNodeProperty : public LogicalOperator {

public:
    LogicalScanNodeProperty(string nodeID, table_id_t tableID, vector<string> propertyNames,
        vector<uint32_t> propertyIDs, shared_ptr<LogicalOperator> child)
        : LogicalOperator{move(child)}, nodeID{move(nodeID)}, tableID{tableID},
          propertyNames{move(propertyNames)}, propertyIDs{move(propertyIDs)} {}

    LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_SCAN_NODE_PROPERTY;
    }

    string getExpressionsForPrinting() const override {
        auto result = string();
        for (auto& propertyName : propertyNames) {
            result += ", " + propertyName;
        }
        return result;
    }

    inline string getNodeID() const { return nodeID; }

    inline table_id_t getTableID() const { return tableID; }

    inline vector<string> getPropertyNames() const { return propertyNames; }

    inline vector<uint32_t> getPropertyIDs() const { return propertyIDs; }

    unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalScanNodeProperty>(
            nodeID, tableID, propertyNames, propertyIDs, children[0]->copy());
    }

private:
    string nodeID;
    table_id_t tableID;
    vector<string> propertyNames;
    vector<uint32_t> propertyIDs;
};

} // namespace planner
} // namespace kuzu

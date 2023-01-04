#pragma once

#include <unordered_map>

#include "expression.h"

namespace kuzu {
namespace binder {

class NodeOrRelExpression : public Expression {
public:
    NodeOrRelExpression(
        DataTypeID dataTypeID, const string& uniqueName, vector<table_id_t> tableIDs)
        : Expression{VARIABLE, dataTypeID, uniqueName}, tableIDs{std::move(tableIDs)} {}
    virtual ~NodeOrRelExpression() = default;

    inline void addTableIDs(const vector<table_id_t>& tableIDsToAdd) {
        auto tableIDsMap = unordered_set<table_id_t>(tableIDs.begin(), tableIDs.end());
        for (auto tableID : tableIDsToAdd) {
            if (!tableIDsMap.contains(tableID)) {
                tableIDs.push_back(tableID);
            }
        }
    }
    inline bool isMultiLabeled() const { return tableIDs.size() > 1; }
    inline vector<table_id_t> getTableIDs() const { return tableIDs; }
    inline table_id_t getSingleTableID() const {
        assert(tableIDs.size() == 1);
        return tableIDs[0];
    }

    inline void addPropertyExpression(const string propertyName, unique_ptr<Expression> property) {
        assert(!propertyNameToIdx.contains(propertyName));
        propertyNameToIdx.insert({propertyName, properties.size()});
        properties.push_back(std::move(property));
    }
    inline bool hasPropertyExpression(const string& propertyName) const {
        return propertyNameToIdx.contains(propertyName);
    }
    inline shared_ptr<Expression> getPropertyExpression(const string& propertyName) const {
        assert(propertyNameToIdx.contains(propertyName));
        return properties[propertyNameToIdx.at(propertyName)]->copy();
    }
    inline const vector<unique_ptr<Expression>>& getPropertyExpressions() const {
        return properties;
    }

protected:
    vector<table_id_t> tableIDs;
    unordered_map<std::string, size_t> propertyNameToIdx;
    vector<unique_ptr<Expression>> properties;
};

} // namespace binder
} // namespace kuzu

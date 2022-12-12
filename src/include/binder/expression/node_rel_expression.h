#pragma once

#include "expression.h"

namespace kuzu {
namespace binder {

class NodeOrRelExpression : public Expression {
public:
    NodeOrRelExpression(
        DataTypeID dataTypeID, const string& uniqueName, vector<table_id_t> tableIDs)
        : Expression{VARIABLE, dataTypeID, uniqueName}, tableIDs{std::move(tableIDs)} {}

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

protected:
    vector<table_id_t> tableIDs;
};

} // namespace binder
} // namespace kuzu

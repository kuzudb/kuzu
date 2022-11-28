#pragma once

#include "property_expression.h"

namespace kuzu {
namespace binder {

class NodeExpression : public Expression {
public:
    NodeExpression(const string& uniqueName, unordered_set<table_id_t> tableIDs)
        : Expression{VARIABLE, NODE, uniqueName}, tableIDs{std::move(tableIDs)} {}

    inline void addTableIDs(const unordered_set<table_id_t>& tableIDsToAdd) {
        tableIDs.insert(tableIDsToAdd.begin(), tableIDsToAdd.end());
    }
    inline uint32_t getNumTableIDs() const { return tableIDs.size(); }
    inline unordered_set<table_id_t> getTableIDs() const { return tableIDs; }
    inline table_id_t getTableID() const {
        assert(tableIDs.size() == 1);
        return *tableIDs.begin();
    }

    inline void setInternalIDProperty(shared_ptr<Expression> expression) {
        internalIDExpression = std::move(expression);
    }
    inline shared_ptr<Expression> getInternalIDProperty() const {
        assert(internalIDExpression != nullptr);
        return internalIDExpression;
    }
    inline string getInternalIDPropertyName() const {
        return internalIDExpression->getUniqueName();
    }

private:
    unordered_set<table_id_t> tableIDs;
    shared_ptr<Expression> internalIDExpression;
};

} // namespace binder
} // namespace kuzu

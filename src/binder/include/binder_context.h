#pragma once

#include <unordered_map>

#include "src/binder/include/expression/expression.h"

namespace graphflow {
namespace binder {

/**
 * Binder context keeps the state of expression binder and query binder. When binding subquery, new
 * binder objects should be created with binder context copied.
 */
class BinderContext {

public:
    BinderContext(uint32_t& lastVariableId) : variablesInScope{}, lastVariableId{lastVariableId} {}

    BinderContext(const BinderContext& other)
        : variablesInScope{other.variablesInScope}, lastVariableId{other.lastVariableId} {}

    inline bool hasVariableInScope() const { return !variablesInScope.empty(); }
    inline const unordered_map<string, shared_ptr<Expression>>& getVariablesInScope() const {
        return variablesInScope;
    }
    inline void setVariablesInScope(
        unordered_map<string, shared_ptr<Expression>> newVariablesInScope) {
        variablesInScope = move(newVariablesInScope);
    }
    inline bool containsVariable(const string& variableName) const {
        return variablesInScope.contains(variableName);
    }
    inline shared_ptr<Expression> getVariable(const string& variableName) const {
        return variablesInScope.at(variableName);
    }
    inline void addVariable(const string& name, shared_ptr<Expression> variable) {
        variablesInScope.insert({name, move(variable)});
    }

    inline string getUniqueVariableName(const string& name) {
        return "_" + to_string(lastVariableId++) + "_" + name;
    }

private:
    unordered_map<string, shared_ptr<Expression>> variablesInScope;
    uint32_t& lastVariableId;
};

} // namespace binder
} // namespace graphflow

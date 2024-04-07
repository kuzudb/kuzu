#pragma once

#include "binder/expression/expression.h"
#include "binder/expression/node_expression.h"

namespace kuzu {
namespace binder {

class BinderScope {
public:
    BinderScope() = default;
    EXPLICIT_COPY_DEFAULT_MOVE(BinderScope);

    bool empty() const { return expressions.empty(); }
    bool contains(const std::string& varName) const { return nameToExprIdx.contains(varName); }
    std::shared_ptr<Expression> getExpression(const std::string& varName) const {
        KU_ASSERT(nameToExprIdx.contains(varName));
        return expressions[nameToExprIdx.at(varName)];
    }
    expression_vector getExpressions() const { return expressions; }
    void addExpression(const std::string& varName, std::shared_ptr<Expression> expression);

    void memorizeTableIDs(const std::string& name, std::vector<common::table_id_t> tableIDs) {
        memorizedNodeNameToTableIDs.insert({name, tableIDs});
    }
    bool hasMemorizedTableIDs(const std::string& name) const {
        return memorizedNodeNameToTableIDs.contains(name);
    }
    std::vector<common::table_id_t> getMemorizedTableIDs(const std::string& name) {
        KU_ASSERT(memorizedNodeNameToTableIDs.contains(name));
        return memorizedNodeNameToTableIDs.at(name);
    }

    void addNodeReplacement(std::shared_ptr<NodeExpression> node) {
        nodeReplacement.insert({node->getVariableName(), node});
    }
    bool hasNodeReplacement(const std::string& name) const {
        return nodeReplacement.contains(name);
    }
    std::shared_ptr<NodeExpression> getNodeReplacement(const std::string& name) const {
        KU_ASSERT(hasNodeReplacement(name));
        return nodeReplacement.at(name);
    }

    void clear();

private:
    BinderScope(const BinderScope& other)
        : expressions{other.expressions}, nameToExprIdx{other.nameToExprIdx},
          memorizedNodeNameToTableIDs{other.memorizedNodeNameToTableIDs} {}

private:
    // Expressions in scope. Order should be preserved.
    expression_vector expressions;
    std::unordered_map<std::string, common::idx_t> nameToExprIdx;
    // A node might be popped out of scope. But we may need to retain its table ID information.
    // E.g. MATCH (a:person) WITH collect(a) AS list_a UNWIND list_a AS new_a MATCH (new_a)-[]->()
    // It will be more performant if we can retain the information that new_a has label person.
    std::unordered_map<std::string, std::vector<common::table_id_t>> memorizedNodeNameToTableIDs;
    // A node pattern may not always be bound as a node expression, e.g. in the above query,
    // (new_a) is bound as a variable rather than node expression.
    std::unordered_map<std::string, std::shared_ptr<NodeExpression>> nodeReplacement;
};

} // namespace binder
} // namespace kuzu

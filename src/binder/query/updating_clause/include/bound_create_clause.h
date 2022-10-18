#pragma once

#include "bound_updating_clause.h"

#include "src/binder/expression/include/rel_expression.h"

namespace graphflow {
namespace binder {

class BoundCreateClause : public BoundUpdatingClause {
public:
    BoundCreateClause(vector<shared_ptr<NodeExpression>> nodes,
        vector<vector<expression_pair>> setItemsPerNode, vector<shared_ptr<RelExpression>> rels,
        vector<vector<expression_pair>> setItemsPerRel)
        : BoundUpdatingClause{ClauseType::CREATE}, nodes{std::move(nodes)},
          setItemsPerNode{std::move(setItemsPerNode)}, rels{std::move(rels)},
          setItemsPerRel{std::move(setItemsPerRel)} {};
    ~BoundCreateClause() override = default;

    inline bool hasNodes() const { return !nodes.empty(); }
    inline uint32_t getNumNodes() const { return nodes.size(); }
    inline vector<shared_ptr<NodeExpression>> getNodes() const { return nodes; }
    inline shared_ptr<NodeExpression> getNode(uint32_t idx) const { return nodes[idx]; }
    inline vector<expression_pair> getNodeSetItems(uint32_t idx) const {
        return setItemsPerNode[idx];
    }

    inline bool hasRels() const { return !rels.empty(); }
    inline vector<shared_ptr<RelExpression>> getRels() const { return rels; }
    inline vector<vector<expression_pair>> getSetItemsPerRel() const { return setItemsPerRel; }

    inline vector<expression_pair> getNodesSetItems() const {
        vector<expression_pair> result;
        for (auto& setItems : setItemsPerNode) {
            result.insert(result.end(), setItems.begin(), setItems.end());
        }
        return result;
    }

    inline vector<expression_pair> getAllRelSetItems() const {
        vector<expression_pair> result;
        for (auto& setItems : setItemsPerRel) {
            result.insert(result.end(), setItems.begin(), setItems.end());
        }
        return result;
    }

    inline vector<expression_pair> getAllSetItems() const {
        vector<expression_pair> result;
        auto nodeSetItems = getNodesSetItems();
        result.insert(result.end(), nodeSetItems.begin(), nodeSetItems.end());
        auto relSetItems = getAllRelSetItems();
        result.insert(result.end(), relSetItems.begin(), relSetItems.end());
        return result;
    }

    inline expression_vector getPropertiesToRead() const override {
        expression_vector result;
        for (auto& setItem : getAllSetItems()) {
            for (auto& property : setItem.second->getSubPropertyExpressions()) {
                result.push_back(property);
            }
        }
        return result;
    }

    unique_ptr<BoundUpdatingClause> copy() override {
        return make_unique<BoundCreateClause>(nodes, setItemsPerNode, rels, setItemsPerRel);
    }

private:
    vector<shared_ptr<NodeExpression>> nodes;
    vector<vector<expression_pair>> setItemsPerNode;
    vector<shared_ptr<RelExpression>> rels;
    vector<vector<expression_pair>> setItemsPerRel;
};

} // namespace binder
} // namespace graphflow

#pragma once

#include "src/binder/include/expression/node_expression.h"

namespace graphflow {
namespace binder {

class RelExpression : public VariableExpression {

public:
    RelExpression(const string& relName, uint32_t id, label_t label, NodeExpression* srcNode,
        NodeExpression* dstNode, uint64_t lowerBound, uint64_t upperBound)
        : VariableExpression{VARIABLE, REL, relName, id}, label{label}, srcNode{srcNode},
          dstNode{dstNode}, lowerBound{lowerBound}, upperBound{upperBound} {}

    inline string getSrcNodeName() const { return srcNode->getInternalName(); }

    inline string getDstNodeName() const { return dstNode->getInternalName(); }

public:
    label_t label;
    NodeExpression* srcNode;
    NodeExpression* dstNode;
    uint64_t lowerBound;
    uint64_t upperBound;
};

} // namespace binder
} // namespace graphflow

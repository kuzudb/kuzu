#pragma once

#include "src/binder/include/expression/node_expression.h"

namespace graphflow {
namespace binder {

class RelExpression : public VariableExpression {

public:
    RelExpression(const string& uniqueName, label_t label, shared_ptr<NodeExpression> srcNode,
        shared_ptr<NodeExpression> dstNode, uint64_t lowerBound, uint64_t upperBound)
        : VariableExpression{VARIABLE, REL, uniqueName}, label{label}, srcNode{move(srcNode)},
          dstNode{move(dstNode)}, lowerBound{lowerBound}, upperBound{upperBound} {}

    inline string getSrcNodeName() const { return srcNode->getInternalName(); }

    inline string getDstNodeName() const { return dstNode->getInternalName(); }

    unique_ptr<Expression> copy() override {
        return make_unique<RelExpression>(
            uniqueName, label, srcNode, dstNode, lowerBound, upperBound);
    }

public:
    label_t label;
    shared_ptr<NodeExpression> srcNode;
    shared_ptr<NodeExpression> dstNode;
    uint64_t lowerBound;
    uint64_t upperBound;
};

} // namespace binder
} // namespace graphflow

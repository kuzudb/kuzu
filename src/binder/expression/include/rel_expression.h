#pragma once

#include "node_expression.h"

namespace graphflow {
namespace binder {

class RelExpression : public Expression {

public:
    RelExpression(const string& uniqueName, table_id_t tableID, shared_ptr<NodeExpression> srcNode,
        shared_ptr<NodeExpression> dstNode, uint64_t lowerBound, uint64_t upperBound)
        : Expression{VARIABLE, REL, uniqueName}, tableID{tableID}, srcNode{move(srcNode)},
          dstNode{move(dstNode)}, lowerBound{lowerBound}, upperBound{upperBound} {}

    inline table_id_t getTableID() const { return tableID; }

    inline shared_ptr<NodeExpression> getSrcNode() const { return srcNode; }

    inline string getSrcNodeName() const { return srcNode->getUniqueName(); }

    inline shared_ptr<NodeExpression> getDstNode() const { return dstNode; }

    inline string getDstNodeName() const { return dstNode->getUniqueName(); }

    inline uint64_t getLowerBound() const { return lowerBound; }

    inline uint64_t getUpperBound() const { return upperBound; }

    inline bool isVariableLength() const { return !(lowerBound == 1 && upperBound == 1); }

private:
    table_id_t tableID;
    shared_ptr<NodeExpression> srcNode;
    shared_ptr<NodeExpression> dstNode;
    uint64_t lowerBound;
    uint64_t upperBound;
};

} // namespace binder
} // namespace graphflow

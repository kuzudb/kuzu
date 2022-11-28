#pragma once

#include "common/exception.h"
#include "node_expression.h"

namespace kuzu {
namespace binder {

class RelExpression : public Expression {
public:
    RelExpression(const string& uniqueName, table_id_t tableID, shared_ptr<NodeExpression> srcNode,
        shared_ptr<NodeExpression> dstNode, uint64_t lowerBound, uint64_t upperBound)
        : Expression{VARIABLE, REL, uniqueName}, tableID{tableID}, srcNode{std::move(srcNode)},
          dstNode{std::move(dstNode)}, lowerBound{lowerBound}, upperBound{upperBound} {}

    inline table_id_t getTableID() const { return tableID; }

    inline shared_ptr<NodeExpression> getSrcNode() const { return srcNode; }

    inline string getSrcNodeName() const { return srcNode->getUniqueName(); }

    inline shared_ptr<NodeExpression> getDstNode() const { return dstNode; }

    inline string getDstNodeName() const { return dstNode->getUniqueName(); }

    inline uint64_t getLowerBound() const { return lowerBound; }

    inline uint64_t getUpperBound() const { return upperBound; }

    inline bool isVariableLength() const { return !(lowerBound == 1 && upperBound == 1); }

    inline void setInternalIDProperty(shared_ptr<Expression> expression) {
        internalIDExpression = std::move(expression);
    }
    inline bool hasInternalIDProperty() const { return internalIDExpression != nullptr; }
    inline shared_ptr<Expression> getInternalIDProperty() const {
        if (!hasInternalIDProperty()) {
            throw NotImplementedException(
                "Cannot read internal ID property for variable length rel " + getRawName());
        }
        return internalIDExpression;
    }

private:
    table_id_t tableID;
    shared_ptr<NodeExpression> srcNode;
    shared_ptr<NodeExpression> dstNode;
    uint64_t lowerBound;
    uint64_t upperBound;
    shared_ptr<Expression> internalIDExpression;
};

} // namespace binder
} // namespace kuzu

#pragma once

#include "common/exception.h"
#include "node_expression.h"

namespace kuzu {
namespace binder {

class RelExpression : public NodeOrRelExpression {
public:
    RelExpression(std::string uniqueName, std::string variableName,
        std::vector<common::table_id_t> tableIDs, std::shared_ptr<NodeExpression> srcNode,
        std::shared_ptr<NodeExpression> dstNode, uint64_t lowerBound, uint64_t upperBound)
        : NodeOrRelExpression{common::REL, std::move(uniqueName), std::move(variableName),
              std::move(tableIDs)},
          srcNode{std::move(srcNode)}, dstNode{std::move(dstNode)}, lowerBound{lowerBound},
          upperBound{upperBound} {}

    inline bool isBoundByMultiLabeledNode() const {
        return srcNode->isMultiLabeled() || dstNode->isMultiLabeled();
    }

    inline std::shared_ptr<NodeExpression> getSrcNode() const { return srcNode; }
    inline std::string getSrcNodeName() const { return srcNode->getUniqueName(); }
    inline std::shared_ptr<NodeExpression> getDstNode() const { return dstNode; }
    inline std::string getDstNodeName() const { return dstNode->getUniqueName(); }

    inline uint64_t getLowerBound() const { return lowerBound; }
    inline uint64_t getUpperBound() const { return upperBound; }
    inline bool isVariableLength() const { return !(lowerBound == 1 && upperBound == 1); }

    inline bool hasInternalIDProperty() const {
        return hasPropertyExpression(common::INTERNAL_ID_SUFFIX);
    }
    inline std::shared_ptr<Expression> getInternalIDProperty() const {
        return getPropertyExpression(common::INTERNAL_ID_SUFFIX);
    }

private:
    std::shared_ptr<NodeExpression> srcNode;
    std::shared_ptr<NodeExpression> dstNode;
    uint64_t lowerBound;
    uint64_t upperBound;
};

} // namespace binder
} // namespace kuzu

#pragma once

#include "common/exception.h"
#include "common/query_rel_type.h"
#include "node_expression.h"

namespace kuzu {
namespace binder {

class RelExpression : public NodeOrRelExpression {
public:
    RelExpression(std::string uniqueName, std::string variableName,
        std::vector<common::table_id_t> tableIDs, std::shared_ptr<NodeExpression> srcNode,
        std::shared_ptr<NodeExpression> dstNode, common::QueryRelType relType, uint64_t lowerBound,
        uint64_t upperBound)
        : NodeOrRelExpression{common::REL, std::move(uniqueName), std::move(variableName),
              std::move(tableIDs)},
          srcNode{std::move(srcNode)}, dstNode{std::move(dstNode)}, relType{relType},
          lowerBound{lowerBound}, upperBound{upperBound} {}

    inline bool isBoundByMultiLabeledNode() const {
        return srcNode->isMultiLabeled() || dstNode->isMultiLabeled();
    }

    inline std::shared_ptr<NodeExpression> getSrcNode() const { return srcNode; }
    inline std::string getSrcNodeName() const { return srcNode->getUniqueName(); }
    inline std::shared_ptr<NodeExpression> getDstNode() const { return dstNode; }
    inline std::string getDstNodeName() const { return dstNode->getUniqueName(); }

    inline common::QueryRelType getRelType() const { return relType; }
    inline uint64_t getLowerBound() const { return lowerBound; }
    inline uint64_t getUpperBound() const { return upperBound; }

    inline bool hasInternalIDProperty() const {
        return hasPropertyExpression(common::INTERNAL_ID_SUFFIX);
    }
    inline std::shared_ptr<Expression> getInternalIDProperty() const {
        return getPropertyExpression(common::INTERNAL_ID_SUFFIX);
    }

    inline void setInternalLengthProperty(std::shared_ptr<Expression> expression) {
        internalLengthExpression = std::move(expression);
    }
    inline std::shared_ptr<Expression> getInternalLengthProperty() {
        return internalLengthExpression;
    }

private:
    std::shared_ptr<NodeExpression> srcNode;
    std::shared_ptr<NodeExpression> dstNode;
    common::QueryRelType relType;
    uint64_t lowerBound;
    uint64_t upperBound;
    std::shared_ptr<Expression> internalLengthExpression;
};

} // namespace binder
} // namespace kuzu

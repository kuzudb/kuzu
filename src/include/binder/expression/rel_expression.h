#pragma once

#include "common/exception.h"
#include "common/query_rel_type.h"
#include "node_expression.h"

namespace kuzu {
namespace binder {

enum class RelDirectionType : uint8_t {
    SINGLE = 0,
    BOTH = 1,
    UNKNOWN = 2,
};

class RelExpression;

struct RecursiveInfo {
    uint64_t lowerBound;
    uint64_t upperBound;
    std::shared_ptr<NodeExpression> node;
    // NodeCopy has the same fields as node but a different unique name.
    // We use nodeCopy to plan recursive plan because boundNode&nbrNode cannot be the same.
    std::shared_ptr<NodeExpression> nodeCopy;
    std::shared_ptr<RelExpression> rel;
    std::shared_ptr<Expression> lengthExpression;
    expression_vector predicates;

    RecursiveInfo(uint64_t lowerBound, uint64_t upperBound, std::shared_ptr<NodeExpression> node,
        std::shared_ptr<NodeExpression> nodeCopy, std::shared_ptr<RelExpression> rel,
        std::shared_ptr<Expression> lengthExpression, expression_vector predicates)
        : lowerBound{lowerBound}, upperBound{upperBound}, node{std::move(node)},
          nodeCopy{std::move(nodeCopy)}, rel{std::move(rel)},
          lengthExpression{std::move(lengthExpression)}, predicates{std::move(predicates)} {}
};

class RelExpression : public NodeOrRelExpression {
public:
    RelExpression(common::LogicalType dataType, std::string uniqueName, std::string variableName,
        std::vector<common::table_id_t> tableIDs, std::shared_ptr<NodeExpression> srcNode,
        std::shared_ptr<NodeExpression> dstNode, RelDirectionType directionType,
        common::QueryRelType relType)
        : NodeOrRelExpression{std::move(dataType), std::move(uniqueName), std::move(variableName),
              std::move(tableIDs)},
          srcNode{std::move(srcNode)}, dstNode{std::move(dstNode)},
          directionType{directionType}, relType{relType} {}

    inline bool isBoundByMultiLabeledNode() const {
        return srcNode->isMultiLabeled() || dstNode->isMultiLabeled();
    }

    inline std::shared_ptr<NodeExpression> getSrcNode() const { return srcNode; }
    inline std::string getSrcNodeName() const { return srcNode->getUniqueName(); }
    inline void setDstNode(std::shared_ptr<NodeExpression> node) { dstNode = std::move(node); }
    inline std::shared_ptr<NodeExpression> getDstNode() const { return dstNode; }
    inline std::string getDstNodeName() const { return dstNode->getUniqueName(); }

    inline common::QueryRelType getRelType() const { return relType; }

    inline RelDirectionType getDirectionType() const { return directionType; }

    inline std::shared_ptr<Expression> getInternalIDProperty() const {
        return getPropertyExpression(common::InternalKeyword::ID);
    }

    inline void setRecursiveInfo(std::unique_ptr<RecursiveInfo> recursiveInfo_) {
        recursiveInfo = std::move(recursiveInfo_);
    }
    inline RecursiveInfo* getRecursiveInfo() const { return recursiveInfo.get(); }
    inline size_t getLowerBound() const { return recursiveInfo->lowerBound; }
    inline size_t getUpperBound() const { return recursiveInfo->upperBound; }
    inline std::shared_ptr<Expression> getLengthExpression() const {
        return recursiveInfo->lengthExpression;
    }

    inline bool isSelfLoop() const { return *srcNode == *dstNode; }

private:
    std::shared_ptr<NodeExpression> srcNode;
    std::shared_ptr<NodeExpression> dstNode;
    RelDirectionType directionType;
    common::QueryRelType relType;
    std::unique_ptr<RecursiveInfo> recursiveInfo;
};

} // namespace binder
} // namespace kuzu

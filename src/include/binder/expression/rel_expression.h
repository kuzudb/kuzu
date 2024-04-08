#pragma once

#include "common/constants.h"
#include "common/enums/query_rel_type.h"
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
    /*
     * E.g. [e*1..2 (r, n) | WHERE n.age > 10 AND r.year = 2012 ]
     * lowerBound = 1
     * upperBound = 2
     * node = n
     * nodeCopy = n (see comment below)
     * rel = r
     * predicates = [n.age > 10, r.year = 2012]
     * */
    uint64_t lowerBound;
    uint64_t upperBound;
    std::shared_ptr<NodeExpression> node;
    // NodeCopy has the same fields as node but a different unique name.
    // We use nodeCopy to plan recursive plan because boundNode&nbrNode cannot be the same.
    std::shared_ptr<NodeExpression> nodeCopy;
    std::shared_ptr<RelExpression> rel;

    std::shared_ptr<Expression> lengthExpression;
    // Node predicate should only be applied to intermediate node. So we need to avoid executing
    // predicate for src and dst node. This is done by rewriting node predicate 'P' as 'Flag OR P'
    // During recursive computation, we set 'Flag' to True to avoid executing 'P'.
    std::shared_ptr<Expression> nodePredicateExecFlag;
    std::shared_ptr<Expression> nodePredicate;
    std::shared_ptr<Expression> relPredicate;
    // Projection list
    expression_vector nodeProjectionList;
    expression_vector relProjectionList;
};

struct RdfPredicateInfo {
    std::vector<common::table_id_t> resourceTableIDs;
    std::shared_ptr<Expression> predicateID;

    RdfPredicateInfo(std::vector<common::table_id_t> resourceTableIDs,
        std::shared_ptr<Expression> predicateID)
        : resourceTableIDs{std::move(resourceTableIDs)}, predicateID{std::move(predicateID)} {}
    DELETE_COPY_DEFAULT_MOVE(RdfPredicateInfo);
};

class RelExpression : public NodeOrRelExpression {
public:
    RelExpression(common::LogicalType dataType, std::string uniqueName, std::string variableName,
        std::vector<common::table_id_t> tableIDs, std::shared_ptr<NodeExpression> srcNode,
        std::shared_ptr<NodeExpression> dstNode, RelDirectionType directionType,
        common::QueryRelType relType)
        : NodeOrRelExpression{std::move(dataType), std::move(uniqueName), std::move(variableName),
              std::move(tableIDs)},
          srcNode{std::move(srcNode)}, dstNode{std::move(dstNode)}, directionType{directionType},
          relType{relType} {}

    inline bool isRecursive() const {
        return dataType.getLogicalTypeID() == common::LogicalTypeID::RECURSIVE_REL;
    }

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

    inline void setRdfPredicateInfo(std::unique_ptr<RdfPredicateInfo> info) {
        rdfPredicateInfo = std::move(info);
    }
    inline RdfPredicateInfo* getRdfPredicateInfo() const { return rdfPredicateInfo.get(); }

    inline bool isSelfLoop() const { return *srcNode == *dstNode; }

private:
    std::shared_ptr<NodeExpression> srcNode;
    std::shared_ptr<NodeExpression> dstNode;
    RelDirectionType directionType;
    common::QueryRelType relType;
    std::unique_ptr<RecursiveInfo> recursiveInfo;
    std::unique_ptr<RdfPredicateInfo> rdfPredicateInfo;
};

} // namespace binder
} // namespace kuzu

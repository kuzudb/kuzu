#pragma once

#include "binder/expression/rel_expression.h"

namespace kuzu {
namespace planner {

enum class JoinNodeType : uint8_t {
    NODE_SCAN = 0,
    REL_SCAN = 1,
    EXPRESSION_SCAN = 2,
    BINARY_JOIN = 5,
    MULTIWAY_JOIN = 6,
};

struct ExtraTreeNodeInfo {
    virtual ~ExtraTreeNodeInfo() = default;

    virtual std::unique_ptr<ExtraTreeNodeInfo> copy() const = 0;

    template<class TARGET>
    const TARGET& constCast() const {
        return common::ku_dynamic_cast<const ExtraTreeNodeInfo&, const TARGET&>(*this);
    }
    template<class TARGET>
    TARGET& cast() {
        return common::ku_dynamic_cast<ExtraTreeNodeInfo&, TARGET&>(*this);
    }
};

struct ExtraJoinTreeNodeInfo : ExtraTreeNodeInfo {
    std::vector<std::shared_ptr<binder::NodeExpression>> joinNodes;
    binder::expression_vector predicates;

    explicit ExtraJoinTreeNodeInfo(std::vector<std::shared_ptr<binder::NodeExpression>> joinNodes)
        : joinNodes{std::move(joinNodes)} {}
    ExtraJoinTreeNodeInfo(const ExtraJoinTreeNodeInfo& other)
        : joinNodes{other.joinNodes}, predicates{other.predicates} {}

    std::unique_ptr<ExtraTreeNodeInfo> copy() const override {
        return std::make_unique<ExtraJoinTreeNodeInfo>(*this);
    }
};

struct NodeTableScanInfo {
    std::shared_ptr<binder::NodeExpression> node;
    binder::expression_vector properties;
    binder::expression_vector predicates;

    NodeTableScanInfo(std::shared_ptr<binder::NodeExpression> node,
        binder::expression_vector properties)
        : node{std::move(node)}, properties{std::move(properties)} {}
};

struct RelTableScanInfo {
    std::shared_ptr<binder::RelExpression> rel;
    binder::expression_vector properties;
    binder::expression_vector predicates;

    RelTableScanInfo(std::shared_ptr<binder::RelExpression> rel,
        binder::expression_vector properties)
        : rel{std::move(rel)}, properties{std::move(properties)} {}
};

struct ExtraScanTreeNodeInfo : ExtraTreeNodeInfo {
    std::unique_ptr<NodeTableScanInfo> nodeInfo;
    std::vector<RelTableScanInfo> relInfos;
    binder::expression_vector predicates;

    ExtraScanTreeNodeInfo() = default;
    ExtraScanTreeNodeInfo(const ExtraScanTreeNodeInfo& other)
        : nodeInfo{std::make_unique<NodeTableScanInfo>(*other.nodeInfo)}, relInfos{other.relInfos} {
    }

    bool isSingleRel() const;

    void merge(const ExtraScanTreeNodeInfo& other);

    std::unique_ptr<ExtraTreeNodeInfo> copy() const override {
        return std::make_unique<ExtraScanTreeNodeInfo>(*this);
    }
};

struct ExtraExprScanTreeNodeInfo : ExtraTreeNodeInfo {
    binder::expression_vector corrExprs;
    binder::expression_vector predicates;

    explicit ExtraExprScanTreeNodeInfo(binder::expression_vector corrExprs)
        : corrExprs{std::move(corrExprs)} {}
    ExtraExprScanTreeNodeInfo(const ExtraExprScanTreeNodeInfo& other)
        : corrExprs{other.corrExprs}, predicates{other.predicates} {}

    std::unique_ptr<ExtraTreeNodeInfo> copy() const override {
        return std::make_unique<ExtraExprScanTreeNodeInfo>(*this);
    }
};

struct JoinTreeNode {
    JoinNodeType type;
    std::unique_ptr<ExtraTreeNodeInfo> extraInfo;

    std::vector<std::shared_ptr<JoinTreeNode>> children;

    JoinTreeNode(JoinNodeType type, std::unique_ptr<ExtraTreeNodeInfo> extraInfo)
        : type{type}, extraInfo{std::move(extraInfo)} {}
    DELETE_COPY_DEFAULT_MOVE(JoinTreeNode);

    void addChild(std::shared_ptr<JoinTreeNode> child) { children.push_back(std::move(child)); }
};

struct JoinTree {
    std::shared_ptr<JoinTreeNode> root;
    uint64_t cardinality;
    uint64_t cost;

    explicit JoinTree(std::shared_ptr<JoinTreeNode> root) : root{root}, cardinality{0}, cost{0} {}

    bool isSingleRel() const;

    JoinTree(const JoinTree& other)
        : root{other.root}, cardinality{other.cardinality}, cost{other.cost} {}
};

} // namespace planner
} // namespace kuzu

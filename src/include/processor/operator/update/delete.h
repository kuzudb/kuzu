#pragma once

#include "processor/operator/physical_operator.h"
#include "storage/store/node_table.h"

namespace kuzu {
namespace processor {

struct DeleteNodeInfo {
    NodeTable* table;
    DataPos nodeIDPos;
    DataPos primaryKeyPos;

    DeleteNodeInfo(NodeTable* table, const DataPos& nodeIDPos, const DataPos& primaryKeyPos)
        : table{table}, nodeIDPos{nodeIDPos}, primaryKeyPos{primaryKeyPos} {}

    inline unique_ptr<DeleteNodeInfo> clone() {
        return make_unique<DeleteNodeInfo>(table, nodeIDPos, primaryKeyPos);
    }
};

class DeleteNode : public PhysicalOperator {
public:
    DeleteNode(vector<unique_ptr<DeleteNodeInfo>> deleteNodeInfos,
        unique_ptr<PhysicalOperator> child, uint32_t id, const string& paramsString)
        : PhysicalOperator{std::move(child), id, paramsString}, deleteNodeInfos{
                                                                    std::move(deleteNodeInfos)} {}

    inline PhysicalOperatorType getOperatorType() override {
        return PhysicalOperatorType::DELETE_NODE;
    }

    shared_ptr<ResultSet> init(ExecutionContext* context) override;

    bool getNextTuplesInternal() override;

    inline unique_ptr<PhysicalOperator> clone() override {
        vector<unique_ptr<DeleteNodeInfo>> clonedDeleteNodeInfos;
        for (auto& deleteNodeInfo : deleteNodeInfos) {
            clonedDeleteNodeInfos.push_back(deleteNodeInfo->clone());
        }
        return make_unique<DeleteNode>(
            std::move(clonedDeleteNodeInfos), children[0]->clone(), id, paramsString);
    }

private:
    vector<unique_ptr<DeleteNodeInfo>> deleteNodeInfos;

    vector<ValueVector*> nodeIDVectors;
    vector<ValueVector*> primaryKeyVectors;
};

struct DeleteRelInfo {
    RelTable* table;
    DataPos srcNodePos;
    table_id_t srcNodeTableID;
    DataPos dstNodePos;
    table_id_t dstNodeTableID;
    DataPos relIDPos;

    DeleteRelInfo(RelTable* table, const DataPos& srcNodePos, table_id_t srcNodeTableID,
        const DataPos& dstNodePos, table_id_t dstNodeTableID, const DataPos& relIDPos)
        : table{table}, srcNodePos{srcNodePos}, srcNodeTableID{srcNodeTableID},
          dstNodePos{dstNodePos}, dstNodeTableID{dstNodeTableID}, relIDPos{relIDPos} {}

    inline unique_ptr<DeleteRelInfo> clone() {
        return make_unique<DeleteRelInfo>(
            table, srcNodePos, srcNodeTableID, dstNodePos, dstNodeTableID, relIDPos);
    }
};

class DeleteRel : public PhysicalOperator {
public:
    DeleteRel(RelsStatistics& relsStatistics, vector<unique_ptr<DeleteRelInfo>> deleteRelInfos,
        unique_ptr<PhysicalOperator> child, uint32_t id, const string& paramsString)
        : PhysicalOperator{std::move(child), id, paramsString}, relsStatistics{relsStatistics},
          deleteRelInfos{std::move(deleteRelInfos)} {}

    inline PhysicalOperatorType getOperatorType() override {
        return PhysicalOperatorType::DELETE_REL;
    }

    shared_ptr<ResultSet> init(ExecutionContext* context) override;

    bool getNextTuplesInternal() override;

    unique_ptr<PhysicalOperator> clone() override {
        vector<unique_ptr<DeleteRelInfo>> clonedDeleteRelInfos;
        for (auto& deleteRelInfo : deleteRelInfos) {
            clonedDeleteRelInfos.push_back(deleteRelInfo->clone());
        }
        return make_unique<DeleteRel>(relsStatistics, std::move(clonedDeleteRelInfos),
            children[0]->clone(), id, paramsString);
    }

private:
    RelsStatistics& relsStatistics;
    vector<unique_ptr<DeleteRelInfo>> deleteRelInfos;
    vector<ValueVector*> srcNodeVectors;
    vector<ValueVector*> dstNodeVectors;
    vector<ValueVector*> relIDVectors;
};

} // namespace processor
} // namespace kuzu

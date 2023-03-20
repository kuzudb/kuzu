#pragma once

#include "processor/operator/physical_operator.h"
#include "storage/store/node_table.h"

namespace kuzu {
namespace processor {

struct DeleteNodeInfo {
    storage::NodeTable* table;
    DataPos nodeIDPos;
    DataPos primaryKeyPos;

    DeleteNodeInfo(
        storage::NodeTable* table, const DataPos& nodeIDPos, const DataPos& primaryKeyPos)
        : table{table}, nodeIDPos{nodeIDPos}, primaryKeyPos{primaryKeyPos} {}

    inline std::unique_ptr<DeleteNodeInfo> clone() {
        return std::make_unique<DeleteNodeInfo>(table, nodeIDPos, primaryKeyPos);
    }
};

class DeleteNode : public PhysicalOperator {
public:
    DeleteNode(std::vector<std::unique_ptr<DeleteNodeInfo>> deleteNodeInfos,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::DELETE_NODE, std::move(child), id, paramsString},
          deleteNodeInfos{std::move(deleteNodeInfos)} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool getNextTuplesInternal(ExecutionContext* context) override;

    inline std::unique_ptr<PhysicalOperator> clone() override {
        std::vector<std::unique_ptr<DeleteNodeInfo>> clonedDeleteNodeInfos;
        for (auto& deleteNodeInfo : deleteNodeInfos) {
            clonedDeleteNodeInfos.push_back(deleteNodeInfo->clone());
        }
        return std::make_unique<DeleteNode>(
            std::move(clonedDeleteNodeInfos), children[0]->clone(), id, paramsString);
    }

private:
    std::vector<std::unique_ptr<DeleteNodeInfo>> deleteNodeInfos;

    std::vector<common::ValueVector*> nodeIDVectors;
    std::vector<common::ValueVector*> primaryKeyVectors;
};

struct DeleteRelInfo {
    storage::RelTable* table;
    DataPos srcNodePos;
    common::table_id_t srcNodeTableID;
    DataPos dstNodePos;
    common::table_id_t dstNodeTableID;
    DataPos relIDPos;

    DeleteRelInfo(storage::RelTable* table, const DataPos& srcNodePos,
        common::table_id_t srcNodeTableID, const DataPos& dstNodePos,
        common::table_id_t dstNodeTableID, const DataPos& relIDPos)
        : table{table}, srcNodePos{srcNodePos}, srcNodeTableID{srcNodeTableID},
          dstNodePos{dstNodePos}, dstNodeTableID{dstNodeTableID}, relIDPos{relIDPos} {}

    inline std::unique_ptr<DeleteRelInfo> clone() {
        return std::make_unique<DeleteRelInfo>(
            table, srcNodePos, srcNodeTableID, dstNodePos, dstNodeTableID, relIDPos);
    }
};

class DeleteRel : public PhysicalOperator {
public:
    DeleteRel(storage::RelsStatistics& relsStatistics,
        std::vector<std::unique_ptr<DeleteRelInfo>> deleteRelInfos,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::DELETE_REL, std::move(child), id, paramsString},
          relsStatistics{relsStatistics}, deleteRelInfos{std::move(deleteRelInfos)} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool getNextTuplesInternal(ExecutionContext* context) override;

    std::unique_ptr<PhysicalOperator> clone() override {
        std::vector<std::unique_ptr<DeleteRelInfo>> clonedDeleteRelInfos;
        for (auto& deleteRelInfo : deleteRelInfos) {
            clonedDeleteRelInfos.push_back(deleteRelInfo->clone());
        }
        return make_unique<DeleteRel>(relsStatistics, std::move(clonedDeleteRelInfos),
            children[0]->clone(), id, paramsString);
    }

private:
    storage::RelsStatistics& relsStatistics;
    std::vector<std::unique_ptr<DeleteRelInfo>> deleteRelInfos;
    std::vector<common::ValueVector*> srcNodeVectors;
    std::vector<common::ValueVector*> dstNodeVectors;
    std::vector<common::ValueVector*> relIDVectors;
};

} // namespace processor
} // namespace kuzu

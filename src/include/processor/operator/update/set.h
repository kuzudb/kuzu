#pragma once

#include "expression_evaluator/base_evaluator.h"
#include "processor/operator/physical_operator.h"
#include "storage/store/node_table.h"
#include "storage/store/rel_table.h"

namespace kuzu {
namespace processor {

class SetNodePropertyInfo {
public:
    SetNodePropertyInfo(storage::NodeTable* table, common::property_id_t propertyID,
        const DataPos& nodeIDPos, std::unique_ptr<evaluator::BaseExpressionEvaluator> evaluator)
        : table{table}, propertyID{propertyID}, nodeIDPos{nodeIDPos}, evaluator{
                                                                          std::move(evaluator)} {}

    inline std::unique_ptr<SetNodePropertyInfo> clone() const {
        return make_unique<SetNodePropertyInfo>(table, propertyID, nodeIDPos, evaluator->clone());
    }

    storage::NodeTable* table;
    common::property_id_t propertyID;
    DataPos nodeIDPos;
    std::unique_ptr<evaluator::BaseExpressionEvaluator> evaluator;
};

class SetNodeProperty : public PhysicalOperator {
public:
    SetNodeProperty(std::vector<std::unique_ptr<SetNodePropertyInfo>> infos,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::SET_NODE_PROPERTY, std::move(child), id,
              paramsString},
          infos{std::move(infos)} {}

    ~SetNodeProperty() override = default;

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool getNextTuplesInternal(ExecutionContext* context) override;

    std::unique_ptr<PhysicalOperator> clone() override;

private:
    std::vector<std::unique_ptr<SetNodePropertyInfo>> infos;

    std::vector<common::ValueVector*> nodeIDVectors;
};

struct SetRelPropertyInfo {
    storage::RelTable* table;
    DataPos srcNodePos;
    DataPos dstNodePos;
    DataPos relIDPos;
    common::property_id_t propertyId;
    std::unique_ptr<evaluator::BaseExpressionEvaluator> evaluator;

    SetRelPropertyInfo(storage::RelTable* table, const DataPos& srcNodePos,
        const DataPos& dstNodePos, const DataPos& relIDPos, common::property_id_t propertyId,
        std::unique_ptr<evaluator::BaseExpressionEvaluator> evaluator)
        : table{table}, srcNodePos{srcNodePos}, dstNodePos{dstNodePos}, relIDPos{relIDPos},
          propertyId{propertyId}, evaluator{std::move(evaluator)} {}

    inline std::unique_ptr<SetRelPropertyInfo> clone() const {
        return make_unique<SetRelPropertyInfo>(
            table, srcNodePos, dstNodePos, relIDPos, propertyId, evaluator->clone());
    }
};

class SetRelProperty : public PhysicalOperator {
public:
    SetRelProperty(std::vector<std::unique_ptr<SetRelPropertyInfo>> infos,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::SET_NODE_PROPERTY, std::move(child), id,
              paramsString},
          infos{std::move(infos)} {}

    ~SetRelProperty() override = default;

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool getNextTuplesInternal(ExecutionContext* context) override;

    std::unique_ptr<PhysicalOperator> clone() override;

private:
    std::vector<std::unique_ptr<SetRelPropertyInfo>> infos;

    std::vector<common::ValueVector*> srcNodeVectors;
    std::vector<common::ValueVector*> dstNodeVectors;
    std::vector<common::ValueVector*> relIDVectors;
};

} // namespace processor
} // namespace kuzu

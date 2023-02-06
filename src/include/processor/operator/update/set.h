#pragma once

#include "expression_evaluator/base_evaluator.h"
#include "processor/operator/physical_operator.h"
#include "storage/storage_structure/column.h"
#include "storage/store/rel_table.h"

namespace kuzu {
namespace processor {

struct SetNodePropertyInfo {
    storage::Column* column;
    DataPos nodeIDPos;
    std::unique_ptr<evaluator::BaseExpressionEvaluator> evaluator;

    SetNodePropertyInfo(storage::Column* column, const DataPos& nodeIDPos,
        std::unique_ptr<evaluator::BaseExpressionEvaluator> evaluator)
        : column{column}, nodeIDPos{nodeIDPos}, evaluator{std::move(evaluator)} {}

    inline std::unique_ptr<SetNodePropertyInfo> clone() const {
        return make_unique<SetNodePropertyInfo>(column, nodeIDPos, evaluator->clone());
    }
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

    bool getNextTuplesInternal() override;

    std::unique_ptr<PhysicalOperator> clone() override;

private:
    std::vector<std::unique_ptr<SetNodePropertyInfo>> infos;

    std::vector<std::shared_ptr<common::ValueVector>> nodeIDVectors;
};

struct SetRelPropertyInfo {
    storage::RelTable* table;
    DataPos srcNodePos;
    DataPos dstNodePos;
    DataPos relIDPos;
    // TODO(Ziyi): see issue 1122 and do a typedef on our column & list idx.
    uint64_t propertyId;
    std::unique_ptr<evaluator::BaseExpressionEvaluator> evaluator;

    SetRelPropertyInfo(storage::RelTable* table, const DataPos& srcNodePos,
        const DataPos& dstNodePos, const DataPos& relIDPos, uint64_t propertyId,
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

    bool getNextTuplesInternal() override;

    std::unique_ptr<PhysicalOperator> clone() override;

private:
    std::vector<std::unique_ptr<SetRelPropertyInfo>> infos;

    std::vector<std::shared_ptr<common::ValueVector>> srcNodeVectors;
    std::vector<std::shared_ptr<common::ValueVector>> dstNodeVectors;
    std::vector<std::shared_ptr<common::ValueVector>> relIDVectors;
};

} // namespace processor
} // namespace kuzu

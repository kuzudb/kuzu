#pragma once

#include "expression_evaluator/expression_evaluator.h"
#include "processor/operator/filtering_operator.h"
#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

class Filter : public PhysicalOperator, public SelVectorOverWriter {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::FILTER;

public:
    Filter(std::unique_ptr<evaluator::ExpressionEvaluator> expressionEvaluator,
        uint32_t dataChunkToSelectPos, std::unique_ptr<PhysicalOperator> child, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : PhysicalOperator{type_, std::move(child), id, std::move(printInfo)},
          expressionEvaluator{std::move(expressionEvaluator)},
          dataChunkToSelectPos(dataChunkToSelectPos) {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool getNextTuplesInternal(ExecutionContext* context) override;

    std::unique_ptr<PhysicalOperator> clone() override {
        return make_unique<Filter>(expressionEvaluator->clone(), dataChunkToSelectPos,
            children[0]->clone(), id, printInfo->copy());
    }

private:
    std::unique_ptr<evaluator::ExpressionEvaluator> expressionEvaluator;
    uint32_t dataChunkToSelectPos;
    std::shared_ptr<common::DataChunk> dataChunkToSelect;
};

struct NodeLabelFilterInfo {
    DataPos nodeVectorPos;
    std::unordered_set<common::table_id_t> nodeLabelSet;

    NodeLabelFilterInfo(const DataPos& nodeVectorPos,
        std::unordered_set<common::table_id_t> nodeLabelSet)
        : nodeVectorPos{nodeVectorPos}, nodeLabelSet{std::move(nodeLabelSet)} {}
    NodeLabelFilterInfo(const NodeLabelFilterInfo& other)
        : nodeVectorPos{other.nodeVectorPos}, nodeLabelSet{other.nodeLabelSet} {}

    inline std::unique_ptr<NodeLabelFilterInfo> copy() const {
        return std::make_unique<NodeLabelFilterInfo>(*this);
    }
};

class NodeLabelFiler : public PhysicalOperator, public SelVectorOverWriter {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::FILTER;

public:
    NodeLabelFiler(std::unique_ptr<NodeLabelFilterInfo> info,
        std::unique_ptr<PhysicalOperator> child, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : PhysicalOperator{type_, std::move(child), id, std::move(printInfo)},
          info{std::move(info)} {}

    void initLocalStateInternal(ResultSet* resultSet_, ExecutionContext* context) override;

    bool getNextTuplesInternal(ExecutionContext* context) override;

    std::unique_ptr<PhysicalOperator> clone() final {
        return std::make_unique<NodeLabelFiler>(info->copy(), children[0]->clone(), id,
            printInfo->copy());
    }

private:
    std::unique_ptr<NodeLabelFilterInfo> info;
    common::ValueVector* nodeIDVector;
};

} // namespace processor
} // namespace kuzu

#pragma once

#include "common/enums/accumulate_type.h"
#include "processor/operator/sink.h"
#include "processor/result/factorized_table.h"

namespace kuzu {
namespace processor {

class ResultCollectorSharedState {
public:
    explicit ResultCollectorSharedState(std::shared_ptr<FactorizedTable> table)
        : table{std::move(table)} {}

    void mergeLocalTable(FactorizedTable& localTable) {
        std::unique_lock lck{mtx};
        table->merge(localTable);
    }

    std::shared_ptr<FactorizedTable> getTable() { return table; }

private:
    std::mutex mtx;
    std::shared_ptr<FactorizedTable> table;
};

struct ResultCollectorInfo {
    common::AccumulateType accumulateType;
    FactorizedTableSchema tableSchema;
    std::vector<DataPos> payloadPositions;

    ResultCollectorInfo(common::AccumulateType accumulateType, FactorizedTableSchema tableSchema,
        std::vector<DataPos> payloadPositions)
        : accumulateType{accumulateType}, tableSchema{std::move(tableSchema)},
          payloadPositions{std::move(payloadPositions)} {}
    EXPLICIT_COPY_DEFAULT_MOVE(ResultCollectorInfo);

private:
    ResultCollectorInfo(const ResultCollectorInfo& other)
        : accumulateType{other.accumulateType}, tableSchema{other.tableSchema.copy()},
          payloadPositions{other.payloadPositions} {}
};

struct ResultCollectorPrintInfo final : OPPrintInfo {
    binder::expression_vector expressions;
    common::AccumulateType accumulateType;

    ResultCollectorPrintInfo(binder::expression_vector expressions,
        common::AccumulateType accumulateType)
        : expressions{std::move(expressions)}, accumulateType{accumulateType} {}
    ResultCollectorPrintInfo(const ResultCollectorPrintInfo& other)
        : OPPrintInfo{other}, expressions{other.expressions}, accumulateType{other.accumulateType} {
    }

    std::string toString() const override;

    std::unique_ptr<OPPrintInfo> copy() const override {
        return std::make_unique<ResultCollectorPrintInfo>(*this);
    }
};

class ResultCollector : public Sink {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::RESULT_COLLECTOR;

public:
    ResultCollector(std::unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        ResultCollectorInfo info, std::shared_ptr<ResultCollectorSharedState> sharedState,
        std::unique_ptr<PhysicalOperator> child, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : Sink{std::move(resultSetDescriptor), type_, std::move(child), id, std::move(printInfo)},
          info{std::move(info)}, sharedState{std::move(sharedState)} {}

    void executeInternal(ExecutionContext* context) final;

    void finalizeInternal(ExecutionContext* context) final;

    std::shared_ptr<FactorizedTable> getResultFactorizedTable() { return sharedState->getTable(); }

    std::unique_ptr<PhysicalOperator> clone() final {
        return make_unique<ResultCollector>(resultSetDescriptor->copy(), info.copy(), sharedState,
            children[0]->clone(), id, printInfo->copy());
    }

private:
    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) final;

private:
    ResultCollectorInfo info;
    std::shared_ptr<ResultCollectorSharedState> sharedState;
    std::vector<common::ValueVector*> payloadVectors;
    std::vector<common::ValueVector*> payloadAndMarkVectors;

    std::unique_ptr<common::ValueVector> markVector;
    std::unique_ptr<FactorizedTable> localTable;
};

} // namespace processor
} // namespace kuzu

#pragma once

#include "common/enums/join_type.h"
#include "processor/operator/sink.h"
#include "processor/result/factorized_table.h"

namespace kuzu {
namespace processor {

class ResultCollectorSharedState {
public:
    explicit ResultCollectorSharedState(std::shared_ptr<FactorizedTable> table)
        : table{std::move(table)} {}

    inline void mergeLocalTable(FactorizedTable& localTable) {
        std::unique_lock lck{mtx};
        table->merge(localTable);
    }

    inline std::shared_ptr<FactorizedTable> getTable() { return table; }

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
    ResultCollectorInfo(const ResultCollectorInfo& other)
        : accumulateType{other.accumulateType}, tableSchema{other.tableSchema.copy()},
          payloadPositions{other.payloadPositions} {}

    inline std::unique_ptr<ResultCollectorInfo> copy() const {
        return std::make_unique<ResultCollectorInfo>(*this);
    }
};

class ResultCollector : public Sink {
public:
    ResultCollector(std::unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        std::unique_ptr<ResultCollectorInfo> info,
        std::shared_ptr<ResultCollectorSharedState> sharedState,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : Sink{std::move(resultSetDescriptor), PhysicalOperatorType::RESULT_COLLECTOR,
              std::move(child), id, paramsString},
          info{std::move(info)}, sharedState{std::move(sharedState)} {}

    void executeInternal(ExecutionContext* context) final;

    void finalize(ExecutionContext* context) final;

    inline std::shared_ptr<FactorizedTable> getResultFactorizedTable() {
        return sharedState->getTable();
    }

    std::unique_ptr<PhysicalOperator> clone() final {
        return make_unique<ResultCollector>(resultSetDescriptor->copy(), info->copy(), sharedState,
            children[0]->clone(), id, paramsString);
    }

private:
    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) final;

private:
    std::unique_ptr<ResultCollectorInfo> info;
    std::shared_ptr<ResultCollectorSharedState> sharedState;
    std::vector<common::ValueVector*> payloadVectors;
    std::vector<common::ValueVector*> payloadAndMarkVectors;

    std::unique_ptr<common::ValueVector> markVector;
    std::unique_ptr<FactorizedTable> localTable;
};

} // namespace processor
} // namespace kuzu

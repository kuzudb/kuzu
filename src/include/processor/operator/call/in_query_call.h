#pragma once

#include "function/table/bind_data.h"
#include "function/table_functions.h"
#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

struct InQueryCallSharedState {
    std::unique_ptr<function::TableFuncSharedState> funcState;
    common::row_idx_t nextRowIdx = 0;
    std::mutex mtx;

    common::row_idx_t getAndIncreaseRowIdx(uint64_t numRows);
};

struct InQueryCallLocalState {
    std::unique_ptr<function::TableFuncLocalState> funcState;
    function::TableFuncInput funcInput;
    function::TableFuncOutput funcOutput;
    common::ValueVector* rowOffsetVector;

    InQueryCallLocalState() = default;
    DELETE_COPY_DEFAULT_MOVE(InQueryCallLocalState);
};

enum class TableScanOutputType : uint8_t {
    EMPTY = 0,
    SINGLE_DATA_CHUNK = 1,
    MULTI_DATA_CHUNK = 2,
};

struct InQueryCallInfo {
    function::TableFunction function;
    std::unique_ptr<function::TableFuncBindData> bindData;
    std::vector<DataPos> outPosV;
    DataPos rowOffsetPos;
    TableScanOutputType outputType;

    InQueryCallInfo() = default;
    EXPLICIT_COPY_DEFAULT_MOVE(InQueryCallInfo);

private:
    InQueryCallInfo(const InQueryCallInfo& other) {
        function = other.function;
        bindData = other.bindData->copy();
        outPosV = other.outPosV;
        rowOffsetPos = other.rowOffsetPos;
        outputType = other.outputType;
    }
};

class InQueryCall : public PhysicalOperator {
public:
    InQueryCall(InQueryCallInfo info, std::shared_ptr<InQueryCallSharedState> sharedState,
        uint32_t id, const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::IN_QUERY_CALL, id, paramsString},
          info{std::move(info)}, sharedState{std::move(sharedState)} {}
    InQueryCall(InQueryCallInfo info, std::shared_ptr<InQueryCallSharedState> sharedState,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::IN_QUERY_CALL, std::move(child), id, paramsString},
          info{std::move(info)}, sharedState{std::move(sharedState)} {}

    InQueryCallSharedState* getSharedState() { return sharedState.get(); }

    bool isSource() const override { return true; }

    bool canParallel() const override { return info.function.canParallelFunc(); }

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    void initGlobalStateInternal(ExecutionContext* context) override;

    bool getNextTuplesInternal(ExecutionContext* context) override;

    double getProgress(ExecutionContext* context) const override;

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<InQueryCall>(info.copy(), sharedState, id, paramsString);
    }

protected:
    InQueryCallInfo info;
    std::shared_ptr<InQueryCallSharedState> sharedState;
    InQueryCallLocalState localState;
};

} // namespace processor
} // namespace kuzu

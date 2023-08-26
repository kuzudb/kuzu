#pragma once

#include "processor/operator/physical_operator.h"
#include "storage/copier/reader_state.h"

namespace kuzu {
namespace processor {

struct ReaderInfo {
    DataPos nodeOffsetPos;
    std::vector<DataPos> dataColumnPoses;
    bool isOrderPreserving;
    storage::read_rows_func_t readFunc;
    storage::init_reader_data_func_t initFunc;
};

class Reader : public PhysicalOperator {
public:
    Reader(ReaderInfo readerInfo, std::shared_ptr<storage::ReaderSharedState> sharedState,
        uint32_t id, const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::READER, id, paramsString}, readerInfo{std::move(
                                                                                readerInfo)},
          sharedState{std::move(sharedState)}, leftNumRows{0}, readFuncData{nullptr} {}

    inline void initGlobalStateInternal(ExecutionContext* context) final {
        sharedState->validate();
        sharedState->countBlocks();
    }
    inline bool isSource() const final { return true; }

    inline std::unique_ptr<PhysicalOperator> clone() final {
        return make_unique<Reader>(readerInfo, sharedState, getOperatorID(), paramsString);
    }

protected:
    bool getNextTuplesInternal(ExecutionContext* context) final;

private:
    void getNextNodeGroupInSerial(std::shared_ptr<arrow::Table>& table);
    void getNextNodeGroupInParallel(std::shared_ptr<arrow::Table>& table);

private:
    ReaderInfo readerInfo;
    std::shared_ptr<storage::ReaderSharedState> sharedState;
    std::vector<std::shared_ptr<arrow::RecordBatch>> leftRecordBatches;
    common::row_idx_t leftNumRows;

    // For parallel reading.
    std::unique_ptr<storage::ReaderFunctionData> readFuncData;
};

} // namespace processor
} // namespace kuzu

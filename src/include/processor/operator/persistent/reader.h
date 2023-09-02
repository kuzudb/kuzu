#pragma once

#include "processor/operator/physical_operator.h"
#include "storage/copier/reader_state.h"

namespace kuzu {
namespace processor {

struct ReaderInfo {
    DataPos nodeOffsetPos;
    std::vector<DataPos> dataColumnsPos;
    bool orderPreserving;

    ReaderInfo(
        const DataPos& nodeOffsetPos, std::vector<DataPos> dataColumnsPos, bool orderPreserving)
        : nodeOffsetPos{nodeOffsetPos}, dataColumnsPos{std::move(dataColumnsPos)},
          orderPreserving{orderPreserving} {}
    ReaderInfo(const ReaderInfo& other)
        : nodeOffsetPos{other.nodeOffsetPos}, dataColumnsPos{other.dataColumnsPos},
          orderPreserving{other.orderPreserving} {}

    inline uint32_t getNumColumns() const { return dataColumnsPos.size(); }

    inline std::unique_ptr<ReaderInfo> copy() const { return std::make_unique<ReaderInfo>(*this); }
};

class Reader : public PhysicalOperator {
public:
    Reader(std::unique_ptr<ReaderInfo> info,
        std::shared_ptr<storage::ReaderSharedState> sharedState, uint32_t id,
        const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::READER, id, paramsString}, info{std::move(info)},
          sharedState{std::move(sharedState)}, dataChunk{nullptr},
          nodeOffsetVector{nullptr}, readFunc{nullptr}, initFunc{nullptr}, readFuncData{nullptr} {}

    void initGlobalStateInternal(ExecutionContext* context) final;

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) final;

    inline bool isSource() const final { return true; }

    inline std::unique_ptr<PhysicalOperator> clone() final {
        return make_unique<Reader>(info->copy(), sharedState, getOperatorID(), paramsString);
    }

protected:
    bool getNextTuplesInternal(ExecutionContext* context) final;

private:
    void getNextNodeGroupInSerial();
    void getNextNodeGroupInParallel();

private:
    std::unique_ptr<ReaderInfo> info;
    std::shared_ptr<storage::ReaderSharedState> sharedState;

    storage::LeftArrowArrays leftArrowArrays;

    std::unique_ptr<common::DataChunk> dataChunk = nullptr;
    common::ValueVector* nodeOffsetVector = nullptr;

    storage::read_rows_func_t readFunc = nullptr;
    storage::init_reader_data_func_t initFunc = nullptr;
    // For parallel reading.
    std::unique_ptr<storage::ReaderFunctionData> readFuncData = nullptr;
};

} // namespace processor
} // namespace kuzu

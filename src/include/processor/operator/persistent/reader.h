#pragma once

#include "processor/operator/persistent/reader_state.h"
#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

struct ReaderInfo {
    DataPos nodeOffsetPos;
    std::vector<DataPos> dataColumnsPos;
    bool containsSerial;

    ReaderInfo(
        const DataPos& nodeOffsetPos, std::vector<DataPos> dataColumnsPos, bool containsSerial)
        : nodeOffsetPos{nodeOffsetPos}, dataColumnsPos{std::move(dataColumnsPos)},
          containsSerial{containsSerial} {}
    ReaderInfo(const ReaderInfo& other)
        : nodeOffsetPos{other.nodeOffsetPos}, dataColumnsPos{other.dataColumnsPos},
          containsSerial{other.containsSerial} {}

    inline uint32_t getNumColumns() const { return dataColumnsPos.size(); }

    inline std::unique_ptr<ReaderInfo> copy() const { return std::make_unique<ReaderInfo>(*this); }
};

class Reader : public PhysicalOperator {
public:
    Reader(std::unique_ptr<ReaderInfo> info, std::shared_ptr<ReaderSharedState> sharedState,
        uint32_t id, const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::READER, id, paramsString}, info{std::move(info)},
          sharedState{std::move(sharedState)}, dataChunk{nullptr},
          nodeOffsetVector{nullptr}, readFunc{nullptr}, initFunc{nullptr}, readFuncData{nullptr} {}

    void initGlobalStateInternal(ExecutionContext* context) final;

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) final;

    inline bool isSource() const final { return true; }

    inline ReaderInfo* getReaderInfo() const { return info.get(); }
    inline ReaderSharedState* getSharedState() const { return sharedState.get(); }

    inline std::unique_ptr<PhysicalOperator> clone() final {
        return make_unique<Reader>(info->copy(), sharedState, getOperatorID(), paramsString);
    }

    inline bool isCopyTurtleFile() const {
        return sharedState->copyDescription->fileType == common::CopyDescription::FileType::TURTLE;
    }

    inline bool getContainsSerial() const { return info->containsSerial; }

protected:
    bool getNextTuplesInternal(ExecutionContext* context) final;

private:
    template<ReaderSharedState::ReadMode READ_MODE>
    void readNextDataChunk();

    template<ReaderSharedState::ReadMode READ_MODE>
    inline void lockForSerial() {
        if constexpr (READ_MODE == ReaderSharedState::ReadMode::SERIAL) {
            sharedState->mtx.lock();
        }
    }
    template<ReaderSharedState::ReadMode READ_MODE>
    inline void unlockForSerial() {
        if constexpr (READ_MODE == ReaderSharedState::ReadMode::SERIAL) {
            sharedState->mtx.unlock();
        }
    }

private:
    std::unique_ptr<ReaderInfo> info;
    std::shared_ptr<ReaderSharedState> sharedState;

    LeftArrowArrays leftArrowArrays;

    std::unique_ptr<common::DataChunk> dataChunk;
    common::ValueVector* nodeOffsetVector;

    read_rows_func_t readFunc;
    init_reader_data_func_t initFunc;
    std::shared_ptr<ReaderFunctionData> readFuncData;
};

} // namespace processor
} // namespace kuzu

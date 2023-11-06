#pragma once

#include <optional>

#include "processor/operator/persistent/reader_state.h"
#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

struct ReaderInfo {
    DataPos offsetPos;
    std::vector<DataPos> dataColumnsPos;

    common::TableType tableType;

    ReaderInfo(const DataPos& internalIDPos, std::vector<DataPos> dataColumnsPos,
        common::TableType tableType)
        : offsetPos{internalIDPos}, dataColumnsPos{std::move(dataColumnsPos)}, tableType{
                                                                                   tableType} {}
    ReaderInfo(const ReaderInfo& other)
        : offsetPos{other.offsetPos}, dataColumnsPos{other.dataColumnsPos}, tableType{
                                                                                other.tableType} {}

    inline uint32_t getNumColumns() const { return dataColumnsPos.size(); }

    inline std::unique_ptr<ReaderInfo> copy() const { return std::make_unique<ReaderInfo>(*this); }
};

class Reader : public PhysicalOperator {
public:
    Reader(std::unique_ptr<ReaderInfo> info, std::shared_ptr<ReaderSharedState> sharedState,
        uint32_t id, const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::READER, id, paramsString}, info{std::move(info)},
          sharedState{std::move(sharedState)}, leftNumRows{0}, dataChunk{nullptr},
          offsetVector{nullptr}, readFunc{nullptr}, initFunc{nullptr}, readFuncData{nullptr} {}

    inline bool isSource() const final { return true; }
    inline bool canParallel() const final {
        return sharedState->readerConfig->fileType != common::FileType::TURTLE;
    }

    void initGlobalStateInternal(ExecutionContext* context) final;

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) final;

    inline ReaderInfo* getReaderInfo() const { return info.get(); }
    inline ReaderSharedState* getSharedState() const { return sharedState.get(); }

    inline std::unique_ptr<PhysicalOperator> clone() final {
        return make_unique<Reader>(info->copy(), sharedState, getOperatorID(), paramsString);
    }

protected:
    bool getNextTuplesInternal(ExecutionContext* context) final;

private:
    template<ReaderSharedState::ReadMode READ_MODE>
    void readNextDataChunk();

    template<ReaderSharedState::ReadMode READ_MODE>
    [[nodiscard]] inline std::optional<std::lock_guard<std::mutex>> lockIfSerial() {
        if constexpr (READ_MODE == ReaderSharedState::ReadMode::SERIAL) {
            return std::make_optional<std::lock_guard<std::mutex>>(sharedState->mtx);
        } else {
            return std::nullopt;
        }
    }

private:
    std::unique_ptr<ReaderInfo> info;
    std::shared_ptr<ReaderSharedState> sharedState;

    common::row_idx_t leftNumRows;

    std::unique_ptr<common::DataChunk> dataChunk;
    common::ValueVector* offsetVector;

    read_rows_func_t readFunc;
    init_reader_data_func_t initFunc;
    std::shared_ptr<ReaderFunctionData> readFuncData;
    storage::MemoryManager* memoryManager;
};

} // namespace processor
} // namespace kuzu

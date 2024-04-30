#pragma once

#include "common/copier_config/csv_reader_config.h"
#include "common/file_system/file_info.h"
#include "common/serializer/buffered_serializer.h"
#include "copy_to.h"
#include "function/scalar_function.h"

namespace kuzu {
namespace processor {

struct CopyToCSVInfo final : public CopyToInfo {
    std::vector<bool> isFlat;
    common::CSVOption copyToOption;

    CopyToCSVInfo(std::vector<std::string> names, std::vector<DataPos> dataPoses,
        std::string fileName, std::vector<bool> isFlat, common::CSVOption copyToOption,
        bool canParallel)
        : CopyToInfo{std::move(names), std::move(dataPoses), std::move(fileName), canParallel},
          isFlat{std::move(isFlat)}, copyToOption{std::move(copyToOption)} {}

    uint64_t getNumFlatVectors() const;

    std::unique_ptr<CopyToInfo> copy() override {
        return std::make_unique<CopyToCSVInfo>(names, dataPoses, fileName, isFlat,
            copyToOption.copy(), canParallel);
    }
};

class CopyToCSVLocalState final : public CopyToLocalState {
public:
    void init(CopyToInfo* info, storage::MemoryManager* mm, ResultSet* resultSet) override;

    void sink(CopyToSharedState* sharedState, CopyToInfo* info) override;

    void finalize(CopyToSharedState* sharedState) override;

    static void writeString(common::BufferedSerializer* serializer, const CopyToCSVInfo* info,
        const uint8_t* strData, uint64_t strLen, bool forceQuote);

private:
    static bool requireQuotes(const CopyToCSVInfo* info, const uint8_t* str, uint64_t len);

    static std::string addEscapes(char toEscape, char escape, const std::string& val);

    void writeRows(const CopyToCSVInfo* info);

private:
    std::unique_ptr<common::BufferedSerializer> serializer;
    std::unique_ptr<common::DataChunk> unflatCastDataChunk;
    std::unique_ptr<common::DataChunk> flatCastDataChunk;
    std::vector<common::ValueVector*> castVectors;
    std::vector<function::scalar_func_exec_t> castFuncs;
    std::vector<std::shared_ptr<common::ValueVector>> vectorsToCast;
};

class CopyToCSVSharedState final : public CopyToSharedState {
public:
    void init(CopyToInfo* info, main::ClientContext* context) override;

    void finalize() override {}

    void writeRows(const uint8_t* data, uint64_t size);

private:
    void writeHeader(CopyToInfo* info);

private:
    std::mutex mtx;
    std::unique_ptr<common::FileInfo> fileInfo;
    common::offset_t offset = 0;
};

class CopyToCSV final : public CopyTo {
public:
    CopyToCSV(std::unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        std::unique_ptr<CopyToInfo> info, std::shared_ptr<CopyToSharedState> sharedState,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : CopyTo{std::move(resultSetDescriptor), std::move(info),
              std::make_unique<CopyToCSVLocalState>(), std::move(sharedState),
              PhysicalOperatorType::COPY_TO, std::move(child), id, paramsString} {}

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<CopyToCSV>(resultSetDescriptor->copy(), info->copy(), sharedState,
            children[0]->clone(), id, paramsString);
    }
};

} // namespace processor
} // namespace kuzu

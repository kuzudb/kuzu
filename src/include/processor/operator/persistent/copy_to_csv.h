#pragma once

#include "common/file_utils.h"
#include "common/serializer/buffered_serializer.h"
#include "copy_to.h"
#include "function/scalar_function.h"

namespace kuzu {
namespace processor {

struct CopyToCSVInfo final : public CopyToInfo {
    std::vector<bool> isFlat;
    std::unique_ptr<common::CSVOption> copyToOption;

    CopyToCSVInfo(std::vector<std::string> names, std::vector<DataPos> dataPoses,
        std::string fileName, std::vector<bool> isFlat,
        std::unique_ptr<common::CSVOption> copyToOption)
        : CopyToInfo{std::move(names), std::move(dataPoses), std::move(fileName)},
          isFlat{std::move(isFlat)}, copyToOption{std::move(copyToOption)} {}

    uint64_t getNumFlatVectors();

    inline std::unique_ptr<CopyToInfo> copy() override {
        return std::make_unique<CopyToCSVInfo>(
            names, dataPoses, fileName, isFlat, copyToOption->copyCSVOption());
    }
};

class CopyToCSVLocalState final : public CopyToLocalState {
public:
    void init(CopyToInfo* info, storage::MemoryManager* mm, ResultSet* resultSet) override;

    void sink(CopyToSharedState* sharedState, CopyToInfo* info) override;

    void finalize(CopyToSharedState* sharedState) override;

private:
    bool requireQuotes(CopyToCSVInfo* info, const uint8_t* str, uint64_t len);

    std::string addEscapes(char toEscape, char escape, const std::string& val);

    void writeString(CopyToCSVInfo* info, const uint8_t* strData, uint64_t strLen, bool forceQuote);

    void writeRows(CopyToCSVInfo* info);

    void writeHeader(CopyToSharedState* sharedState, CopyToCSVInfo* info);

private:
    std::unique_ptr<common::BufferedSerializer> serializer;
    std::unique_ptr<common::DataChunk> unflatCastDataChunk;
    std::unique_ptr<common::DataChunk> flatCastDataChunk;
    std::vector<common::ValueVector*> castVectors;
    std::vector<function::scalar_exec_func> castFuncs;
    std::vector<std::shared_ptr<common::ValueVector>> vectorsToCast;
};

class CopyToCSVSharedState final : public CopyToSharedState {
public:
    void init(CopyToInfo* info, storage::MemoryManager* mm) override;

    void finalize() override {}

    void writeRows(const uint8_t* data, uint64_t size);

    bool writeHeader();

private:
    std::mutex mtx;
    std::unique_ptr<common::FileInfo> fileInfo;
    common::offset_t offset = 0;
    bool outputHeader;
};

class CopyToCSV final : public CopyTo {
public:
    CopyToCSV(std::unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        std::unique_ptr<CopyToInfo> info, std::shared_ptr<CopyToSharedState> sharedState,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : CopyTo{std::move(resultSetDescriptor), std::move(info),
              std::make_unique<CopyToCSVLocalState>(), std::move(sharedState),
              PhysicalOperatorType::COPY_TO_CSV, std::move(child), id, paramsString} {}

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<CopyToCSV>(resultSetDescriptor->copy(), info->copy(), sharedState,
            children[0]->clone(), id, paramsString);
    }
};

} // namespace processor
} // namespace kuzu

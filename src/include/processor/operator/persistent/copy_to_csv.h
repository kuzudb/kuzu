#pragma once

#include "common/file_utils.h"
#include "common/serializer/buffered_serializer.h"
#include "copy_to.h"
#include "function/scalar_function.h"

namespace kuzu {
namespace processor {

struct CopyToCSVInfo : public CopyToInfo {
    std::vector<bool> isFlat;

    CopyToCSVInfo(std::vector<std::string> names, std::vector<DataPos> dataPoses,
        std::string fileName, std::vector<bool> isFlat)
        : CopyToInfo{std::move(names), std::move(dataPoses), std::move(fileName)}, isFlat{std::move(
                                                                                       isFlat)} {}

    uint64_t getNumFlatVectors();

    inline std::unique_ptr<CopyToInfo> copy() override {
        return std::make_unique<CopyToCSVInfo>(names, dataPoses, fileName, isFlat);
    }
};

class CopyToCSVLocalState : public CopyToLocalState {
public:
    void init(CopyToInfo* info, storage::MemoryManager* mm, ResultSet* resultSet) final;

    void sink(CopyToSharedState* sharedState) final;

    void finalize(CopyToSharedState* sharedState) final;

private:
    bool requireQuotes(const uint8_t* str, uint64_t len);

    std::string addEscapes(const char* toEscape, const char* escape, const std::string& val);

    void writeString(const uint8_t* strData, uint64_t strLen, bool forceQuote);

    void writeRows();

private:
    std::unique_ptr<common::BufferedSerializer> serializer;
    std::unique_ptr<common::DataChunk> unflatCastDataChunk;
    std::unique_ptr<common::DataChunk> flatCastDataChunk;
    std::vector<common::ValueVector*> castVectors;
    std::vector<function::scalar_exec_func> castFuncs;
    std::vector<std::shared_ptr<common::ValueVector>> vectorsToCast;
};

class CopyToCSVSharedState : public CopyToSharedState {
public:
    void init(CopyToInfo* info, storage::MemoryManager* mm) final;

    void finalize() final {}

    void writeRows(const uint8_t* data, uint64_t size);

private:
    std::mutex mtx;
    std::unique_ptr<common::FileInfo> fileInfo;
    common::offset_t offset = 0;
};

class CopyToCSV : public CopyTo {
public:
    CopyToCSV(std::unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        std::unique_ptr<CopyToInfo> info, std::shared_ptr<CopyToSharedState> sharedState,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : CopyTo{std::move(resultSetDescriptor), std::move(info),
              std::make_unique<CopyToCSVLocalState>(), std::move(sharedState),
              PhysicalOperatorType::COPY_TO_CSV, std::move(child), id, paramsString} {}

    inline std::unique_ptr<PhysicalOperator> clone() final {
        return std::make_unique<CopyToCSV>(resultSetDescriptor->copy(), info->copy(), sharedState,
            children[0]->clone(), id, paramsString);
    }
};

} // namespace processor
} // namespace kuzu

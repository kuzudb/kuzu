#pragma once

#include "copy_to.h"
#include "parquet/parquet_types.h"
#include "processor/operator/persistent/writer/parquet/parquet_writer.h"
#include "processor/result/factorized_table.h"

namespace kuzu {
namespace processor {

struct CopyToParquetInfo final : public CopyToInfo {
    kuzu_parquet::format::CompressionCodec::type codec =
        kuzu_parquet::format::CompressionCodec::SNAPPY;
    std::unique_ptr<FactorizedTableSchema> tableSchema;
    std::vector<std::unique_ptr<common::LogicalType>> types;

    CopyToParquetInfo(std::unique_ptr<FactorizedTableSchema> tableSchema,
        std::vector<std::unique_ptr<common::LogicalType>> types, std::vector<std::string> names,
        std::vector<DataPos> dataPoses, std::string fileName)
        : tableSchema{std::move(tableSchema)}, types{std::move(types)}, CopyToInfo{std::move(names),
                                                                            std::move(dataPoses),
                                                                            std::move(fileName)} {}

    inline std::unique_ptr<CopyToInfo> copy() override {
        return std::make_unique<CopyToParquetInfo>(
            tableSchema->copy(), common::LogicalType::copy(types), names, dataPoses, fileName);
    }
};

class CopyToParquetLocalState final : public CopyToLocalState {
    void init(CopyToInfo* info, storage::MemoryManager* mm, ResultSet* resultSet) override;

    void sink(CopyToSharedState* sharedState, CopyToInfo* info) override;

    void finalize(CopyToSharedState* sharedState) override;

private:
    std::unique_ptr<FactorizedTable> ft;
    std::vector<common::ValueVector*> vectorsToAppend;
    storage::MemoryManager* mm;
};

class CopyToParquetSharedState final : public CopyToSharedState {
public:
    void init(CopyToInfo* info, storage::MemoryManager* mm) override;

    void finalize() override;

    void flush(FactorizedTable& ft);

private:
    std::unique_ptr<ParquetWriter> writer;
};

class CopyToParquet final : public CopyTo {
public:
    CopyToParquet(std::unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        std::unique_ptr<CopyToInfo> info, std::shared_ptr<CopyToSharedState> sharedState,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : CopyTo{std::move(resultSetDescriptor), std::move(info),
              std::make_unique<CopyToParquetLocalState>(), std::move(sharedState),
              PhysicalOperatorType::COPY_TO_PARQUET, std::move(child), id, paramsString} {}

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<CopyToParquet>(resultSetDescriptor->copy(), info->copy(),
            sharedState, children[0]->clone(), id, paramsString);
    }
};

} // namespace processor
} // namespace kuzu

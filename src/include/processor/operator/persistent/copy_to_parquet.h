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
    FactorizedTableSchema tableSchema;
    std::vector<std::unique_ptr<common::LogicalType>> types;
    DataPos countingVecPos;

    CopyToParquetInfo(FactorizedTableSchema tableSchema,
        std::vector<std::unique_ptr<common::LogicalType>> types, std::vector<std::string> names,
        std::vector<DataPos> dataPoses, std::string fileName, DataPos countingVecPos,
        bool canParallel)
        : CopyToInfo{std::move(names), std::move(dataPoses), std::move(fileName), canParallel},
          tableSchema{std::move(tableSchema)}, types{std::move(types)},
          countingVecPos{std::move(countingVecPos)} {}

    std::unique_ptr<CopyToInfo> copy() override {
        return std::make_unique<CopyToParquetInfo>(tableSchema.copy(),
            common::LogicalType::copy(types), names, dataPoses, fileName, countingVecPos,
            canParallel);
    }
};

class CopyToParquetLocalState final : public CopyToLocalState {
    void init(CopyToInfo* info, storage::MemoryManager* mm, ResultSet* resultSet) override;

    void sink(CopyToSharedState* sharedState, CopyToInfo* info) override;

    void finalize(CopyToSharedState* sharedState) override;

private:
    std::unique_ptr<FactorizedTable> ft;
    uint64_t numTuplesInFT;
    std::vector<common::ValueVector*> vectorsToAppend;
    storage::MemoryManager* mm;
    common::ValueVector* countingVec;
};

class CopyToParquetSharedState final : public CopyToSharedState {
public:
    void init(CopyToInfo* info, main::ClientContext* context) override;

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
              PhysicalOperatorType::COPY_TO, std::move(child), id, paramsString} {}

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<CopyToParquet>(resultSetDescriptor->copy(), info->copy(),
            sharedState, children[0]->clone(), id, paramsString);
    }
};

} // namespace processor
} // namespace kuzu

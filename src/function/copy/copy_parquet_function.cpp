
#include "function/copy/copy_function.h"
#include "main/client_context.h"
#include "parquet/parquet_types.h"
#include "processor/operator/persistent/writer/parquet/parquet_writer.h"
#include "processor/result/factorized_table.h"
#include "processor/result/factorized_table_util.h"
#include "processor/result/result_set.h"

namespace kuzu {
namespace function {

using namespace common;
using namespace processor;

struct CopyToParquetBindData final : public CopyFuncBindData {
    kuzu_parquet::format::CompressionCodec::type codec =
        kuzu_parquet::format::CompressionCodec::SNAPPY;
    FactorizedTableSchema tableSchema;
    DataPos countingVecPos;

    CopyToParquetBindData(std::vector<std::string> names, std::vector<common::LogicalType> types,
        std::string fileName, bool canParallel)
        : CopyFuncBindData{std::move(names), std::move(types), std::move(fileName), canParallel} {}

    CopyToParquetBindData(std::vector<std::string> names, std::vector<common::LogicalType> types,
        std::string fileName, bool canParallel, std::vector<processor::DataPos> dataPoses,
        std::vector<bool> isFlat, FactorizedTableSchema tableSchema, DataPos countingVecPos)
        : CopyFuncBindData{std::move(names), std::move(types), std::move(fileName), canParallel,
              std::move(dataPoses), std::move(isFlat)},
          tableSchema{std::move(tableSchema)}, countingVecPos{std::move(countingVecPos)} {}

    void bindDataPos(planner::Schema* childSchema, std::vector<DataPos> vectorsToCopyPos,
        std::vector<bool> isFlat) override {
        auto expressions = childSchema->getExpressionsInScope();
        tableSchema = FactorizedTableUtils::createFTableSchema(expressions, *childSchema);
        countingVecPos = DataPos(childSchema->getExpressionPos(*expressions[0]));
        for (auto& e : expressions) {
            auto group = childSchema->getGroup(e->getUniqueName());
            if (!group->isFlat()) {
                countingVecPos = DataPos(childSchema->getExpressionPos(*e));
                break;
            }
        }
        this->dataPoses = std::move(vectorsToCopyPos);
        this->isFlat = std::move(isFlat);
    }

    std::unique_ptr<CopyFuncBindData> copy() const override {
        return std::make_unique<CopyToParquetBindData>(names, types, fileName, canParallel,
            dataPoses, isFlat, tableSchema.copy(), countingVecPos);
    }
};

struct CopyToParquetLocalState final : public CopyFuncLocalState {
    std::unique_ptr<FactorizedTable> ft;
    uint64_t numTuplesInFT;
    std::vector<common::ValueVector*> vectorsToAppend;
    storage::MemoryManager* mm;
    common::ValueVector* countingVec;

    CopyToParquetLocalState(const CopyFuncBindData& bindData, main::ClientContext& context,
        const ResultSet& resultSet)
        : mm{context.getMemoryManager()} {
        auto& copyToBindData = bindData.constCast<CopyToParquetBindData>();
        ft = std::make_unique<FactorizedTable>(mm, copyToBindData.tableSchema.copy());
        numTuplesInFT = 0;
        countingVec = nullptr;
        vectorsToAppend.reserve(copyToBindData.dataPoses.size());
        countingVec = resultSet.getValueVector(copyToBindData.countingVecPos).get();
        for (auto& pos : copyToBindData.dataPoses) {
            vectorsToAppend.push_back(resultSet.getValueVector(pos).get());
        }
    }
};

struct CopyToParquetSharedState : public CopyFuncSharedState {
    std::unique_ptr<ParquetWriter> writer;

    CopyToParquetSharedState(const CopyFuncBindData& bindData, main::ClientContext& context) {
        auto& copyToBindData = bindData.constCast<CopyToParquetBindData>();
        writer = std::make_unique<ParquetWriter>(copyToBindData.fileName, copyToBindData.types,
            copyToBindData.names, copyToBindData.codec, &context);
    }
};

static std::unique_ptr<CopyFuncBindData> bindFunc(CopyFuncBindInput& bindInput) {
    return std::make_unique<CopyToParquetBindData>(bindInput.columnNames, bindInput.types,
        bindInput.filePath, true /* canParallel */);
}

static std::unique_ptr<CopyFuncLocalState> initLocalState(main::ClientContext& context,
    const CopyFuncBindData& bindData, const processor::ResultSet& resultSet) {
    return std::make_unique<CopyToParquetLocalState>(bindData, context, resultSet);
}

static std::shared_ptr<CopyFuncSharedState> initSharedState(main::ClientContext& context,
    const CopyFuncBindData& bindData) {
    return std::make_shared<CopyToParquetSharedState>(bindData, context);
}

static void sinkFunc(CopyFuncSharedState& sharedState, CopyFuncLocalState& localState,
    const CopyFuncBindData& /*bindData*/) {
    auto& copyToLocalState = localState.cast<CopyToParquetLocalState>();
    copyToLocalState.ft->append(copyToLocalState.vectorsToAppend);
    copyToLocalState.numTuplesInFT +=
        copyToLocalState.countingVec->state->getSelVector().getSelSize();
    if (copyToLocalState.numTuplesInFT > StorageConstants::NODE_GROUP_SIZE) {
        auto& copyToSharedState = sharedState.cast<CopyToParquetSharedState>();
        copyToSharedState.writer->flush(*copyToLocalState.ft);
        copyToLocalState.numTuplesInFT = 0;
    }
}

static void combineFunc(CopyFuncSharedState& sharedState, CopyFuncLocalState& localState) {
    auto& copyToSharedState = sharedState.cast<CopyToParquetSharedState>();
    auto& copyToLocalState = localState.cast<CopyToParquetLocalState>();
    copyToSharedState.writer->flush(*copyToLocalState.ft);
}

static void finalizeFunc(CopyFuncSharedState& sharedState) {
    auto& copyToSharedState = sharedState.cast<CopyToParquetSharedState>();
    copyToSharedState.writer->finalize();
}

function_set CopyParquetFunction::getFunctionSet() {
    function_set functionSet;
    auto copyFunc = std::make_unique<CopyFunction>(name);
    copyFunc->copyToInitLocal = initLocalState;
    copyFunc->copyToInitShared = initSharedState;
    copyFunc->copyToSink = sinkFunc;
    copyFunc->copyToCombine = combineFunc;
    copyFunc->copyToFinalize = finalizeFunc;
    copyFunc->copyToBind = bindFunc;
    functionSet.push_back(std::move(copyFunc));
    return functionSet;
}

} // namespace function
} // namespace kuzu

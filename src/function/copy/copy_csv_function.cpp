#include <fcntl.h>

#include "common/file_system/virtual_file_system.h"
#include "common/serializer/buffered_serializer.h"
#include "function/cast/vector_cast_functions.h"
#include "function/copy/copy_function.h"
#include "function/scalar_function.h"
#include "main/client_context.h"
#include "processor/result/result_set.h"

namespace kuzu {
namespace function {

using namespace common;

struct CopyToCSVBindData : public CopyFuncBindData {
    CSVOption copyToOption;

    CopyToCSVBindData(std::vector<std::string> names, std::vector<common::LogicalType> types,
        std::string fileName, CSVOption copyToOption, bool canParallel)
        : CopyFuncBindData{std::move(names), std::move(types), std::move(fileName), canParallel},
          copyToOption{std::move(copyToOption)} {}

    CopyToCSVBindData(std::vector<std::string> names, std::vector<common::LogicalType> types,
        std::string fileName, CSVOption copyToOption, bool canParallel,
        std::vector<processor::DataPos> dataPoses, std::vector<bool> isFlat)
        : CopyFuncBindData{std::move(names), std::move(types), std::move(fileName), canParallel,
              std::move(dataPoses), std::move(isFlat)},
          copyToOption{std::move(copyToOption)} {}

    uint64_t getNumFlatVectors() const {
        return std::count(isFlat.begin(), isFlat.end(), true /* isFlat */);
    }

    void bindDataPos(planner::Schema* /*childSchema*/,
        std::vector<processor::DataPos> vectorsToCopyPos, std::vector<bool> isFlat) override {
        this->dataPoses = std::move(vectorsToCopyPos);
        this->isFlat = std::move(isFlat);
    }

    std::unique_ptr<CopyFuncBindData> copy() const override {
        return std::make_unique<CopyToCSVBindData>(names, types, fileName, copyToOption.copy(),
            canParallel, dataPoses, isFlat);
    }
};

static std::string addEscapes(char toEscape, char escape, const std::string& val) {
    uint64_t i = 0;
    std::string escapedStr = "";
    auto found = val.find(toEscape);

    while (found != std::string::npos) {
        while (i < found) {
            escapedStr += val[i];
            i++;
        }
        escapedStr += escape;
        found = val.find(toEscape, found + sizeof(escape));
    }
    while (i < val.length()) {
        escapedStr += val[i];
        i++;
    }
    return escapedStr;
}

static bool requireQuotes(const CopyToCSVBindData& copyToCSVBindData, const uint8_t* str,
    uint64_t len) {
    // Check if the string is equal to the null string.
    if (len == strlen(CopyToCSVConstants::DEFAULT_NULL_STR) &&
        memcmp(str, CopyToCSVConstants::DEFAULT_NULL_STR, len) == 0) {
        return true;
    }
    for (auto i = 0u; i < len; i++) {
        if (str[i] == copyToCSVBindData.copyToOption.quoteChar ||
            str[i] == CopyToCSVConstants::DEFAULT_CSV_NEWLINE[0] ||
            str[i] == copyToCSVBindData.copyToOption.delimiter) {
            return true;
        }
    }
    return false;
}

static void writeString(common::BufferedSerializer* serializer, const CopyFuncBindData& bindData,
    const uint8_t* strData, uint64_t strLen, bool forceQuote) {
    auto& copyToCSVBindData = bindData.constCast<CopyToCSVBindData>();
    if (!forceQuote) {
        forceQuote = requireQuotes(copyToCSVBindData, strData, strLen);
    }
    if (forceQuote) {
        bool requiresEscape = false;
        for (auto i = 0u; i < strLen; i++) {
            if (strData[i] == copyToCSVBindData.copyToOption.quoteChar ||
                strData[i] == copyToCSVBindData.copyToOption.escapeChar) {
                requiresEscape = true;
                break;
            }
        }

        if (!requiresEscape) {
            serializer->writeBufferData(copyToCSVBindData.copyToOption.quoteChar);
            serializer->write(strData, strLen);
            serializer->writeBufferData(copyToCSVBindData.copyToOption.quoteChar);
            return;
        }

        std::string strValToWrite = std::string(reinterpret_cast<const char*>(strData), strLen);
        strValToWrite = addEscapes(copyToCSVBindData.copyToOption.escapeChar,
            copyToCSVBindData.copyToOption.escapeChar, strValToWrite);
        if (copyToCSVBindData.copyToOption.escapeChar != copyToCSVBindData.copyToOption.quoteChar) {
            strValToWrite = addEscapes(copyToCSVBindData.copyToOption.quoteChar,
                copyToCSVBindData.copyToOption.escapeChar, strValToWrite);
        }
        serializer->writeBufferData(copyToCSVBindData.copyToOption.quoteChar);
        serializer->writeBufferData(strValToWrite);
        serializer->writeBufferData(copyToCSVBindData.copyToOption.quoteChar);
    } else {
        serializer->write(strData, strLen);
    }
}

struct CopyToCSVSharedState : public CopyFuncSharedState {
    std::mutex mtx;
    std::unique_ptr<common::FileInfo> fileInfo;
    common::offset_t offset = 0;

    CopyToCSVSharedState(main::ClientContext& context, const CopyFuncBindData& bindData) {
        fileInfo = context.getVFSUnsafe()->openFile(bindData.fileName, O_WRONLY | O_CREAT | O_TRUNC,
            &context);
        writeHeader(bindData);
    }

    void writeHeader(const CopyFuncBindData& bindData) {
        auto& copyToCSVBindData = bindData.constCast<CopyToCSVBindData>();
        BufferedSerializer bufferedSerializer;
        if (copyToCSVBindData.copyToOption.hasHeader) {
            for (auto i = 0u; i < copyToCSVBindData.names.size(); i++) {
                if (i != 0) {
                    bufferedSerializer.writeBufferData(copyToCSVBindData.copyToOption.delimiter);
                }
                auto& name = copyToCSVBindData.names[i];
                writeString(&bufferedSerializer, copyToCSVBindData,
                    reinterpret_cast<const uint8_t*>(name.c_str()), name.length(),
                    false /* forceQuote */);
            }
            bufferedSerializer.writeBufferData(CopyToCSVConstants::DEFAULT_CSV_NEWLINE);
            writeRows(bufferedSerializer.getBlobData(), bufferedSerializer.getSize());
        }
    }

    void writeRows(const uint8_t* data, uint64_t size) {
        std::lock_guard<std::mutex> lck(mtx);
        fileInfo->writeFile(data, size, offset);
        offset += size;
    }
};

struct CopyToCSVLocalState final : public CopyFuncLocalState {
    std::unique_ptr<BufferedSerializer> serializer;
    std::unique_ptr<DataChunk> unflatCastDataChunk;
    std::unique_ptr<DataChunk> flatCastDataChunk;
    std::vector<ValueVector*> castVectors;
    std::vector<function::scalar_func_exec_t> castFuncs;
    std::vector<std::shared_ptr<ValueVector>> vectorsToCast;

    CopyToCSVLocalState(main::ClientContext& context, const CopyFuncBindData& bindData,
        const processor::ResultSet& resultSet) {
        auto& copyToCSVBindData = bindData.constCast<CopyToCSVBindData>();
        serializer = std::make_unique<BufferedSerializer>();
        vectorsToCast.reserve(copyToCSVBindData.dataPoses.size());
        auto numFlatVectors = copyToCSVBindData.getNumFlatVectors();
        unflatCastDataChunk =
            std::make_unique<DataChunk>(copyToCSVBindData.isFlat.size() - numFlatVectors);
        flatCastDataChunk = std::make_unique<DataChunk>(numFlatVectors,
            DataChunkState::getSingleValueDataChunkState());
        uint64_t numInsertedFlatVector = 0;
        castFuncs.resize(copyToCSVBindData.dataPoses.size());
        for (auto i = 0u; i < copyToCSVBindData.dataPoses.size(); i++) {
            auto vectorToCast = resultSet.getValueVector(copyToCSVBindData.dataPoses[i]);
            castFuncs[i] = function::CastFunction::bindCastFunction("cast", vectorToCast->dataType,
                *LogicalType::STRING())
                               ->execFunc;
            vectorsToCast.push_back(std::move(vectorToCast));
            auto castVector =
                std::make_unique<ValueVector>(LogicalTypeID::STRING, context.getMemoryManager());
            castVectors.push_back(castVector.get());
            if (copyToCSVBindData.isFlat[i]) {
                flatCastDataChunk->insert(numInsertedFlatVector, std::move(castVector));
                numInsertedFlatVector++;
            } else {
                unflatCastDataChunk->insert(i - numInsertedFlatVector, std::move(castVector));
            }
        }
    }

    void writeRows(const CopyToCSVBindData& copyToCSVBindData) {
        for (auto i = 0u; i < vectorsToCast.size(); i++) {
            std::vector<std::shared_ptr<ValueVector>> vectorToCast = {vectorsToCast[i]};
            castFuncs[i](vectorToCast, *castVectors[i], nullptr);
        }

        uint64_t numRowsToWrite = 1;
        for (auto& vectorToCast : vectorsToCast) {
            if (!vectorToCast->state->isFlat()) {
                numRowsToWrite = vectorToCast->state->getSelVector().getSelSize();
            }
        }
        for (auto i = 0u; i < numRowsToWrite; i++) {
            for (auto j = 0u; j < castVectors.size(); j++) {
                if (j != 0) {
                    serializer->writeBufferData(copyToCSVBindData.copyToOption.delimiter);
                }
                auto vector = castVectors[j];
                auto pos = vector->state->isFlat() ? vector->state->getSelVector()[0] :
                                                     vector->state->getSelVector()[i];
                if (vector->isNull(pos)) {
                    // write null value
                    serializer->writeBufferData(CopyToCSVConstants::DEFAULT_NULL_STR);
                    continue;
                }
                auto strValue = vector->getValue<ku_string_t>(pos);
                // Note: we need blindly add quotes to LIST.
                writeString(serializer.get(), copyToCSVBindData, strValue.getData(), strValue.len,
                    CopyToCSVConstants::DEFAULT_FORCE_QUOTE ||
                        vectorsToCast[j]->dataType.getLogicalTypeID() == LogicalTypeID::LIST);
            }
            serializer->writeBufferData(CopyToCSVConstants::DEFAULT_CSV_NEWLINE);
        }
    }

    void sink(CopyFuncSharedState& sharedState, const CopyFuncBindData& bindData) {
        auto& copyToCSVBindData = bindData.constCast<CopyToCSVBindData>();
        writeRows(copyToCSVBindData);
        if (serializer->getSize() > CopyToCSVConstants::DEFAULT_CSV_FLUSH_SIZE) {
            auto& copyToSharedState = sharedState.cast<CopyToCSVSharedState>();
            copyToSharedState.writeRows(serializer->getBlobData(), serializer->getSize());
            serializer->reset();
        }
    }
};

static std::unique_ptr<CopyFuncBindData> bindFunc(CopyFuncBindInput& bindInput) {
    return std::make_unique<CopyToCSVBindData>(bindInput.columnNames, bindInput.types,
        bindInput.filePath,
        CSVReaderConfig::construct(std::move(bindInput.parsingOptions)).option.copy(),
        true /* canParallel */);
}

static std::unique_ptr<CopyFuncLocalState> initLocalState(main::ClientContext& context,
    const CopyFuncBindData& bindData, const processor::ResultSet& resultSet) {
    return std::make_unique<CopyToCSVLocalState>(context, bindData, resultSet);
}

static std::shared_ptr<CopyFuncSharedState> initSharedState(main::ClientContext& context,
    const CopyFuncBindData& bindData) {
    return std::make_shared<CopyToCSVSharedState>(context, bindData);
}

static void sinkFunc(CopyFuncSharedState& sharedState, CopyFuncLocalState& localState,
    const CopyFuncBindData& bindData) {
    auto& copyToCSVLocalState = localState.cast<CopyToCSVLocalState>();
    copyToCSVLocalState.sink(sharedState, bindData);
}

static void combineFunc(CopyFuncSharedState& sharedState, CopyFuncLocalState& localState) {
    auto& serializer = localState.cast<CopyToCSVLocalState>().serializer;
    auto& copyToSharedState = sharedState.cast<CopyToCSVSharedState>();
    if (serializer->getSize() > 0) {
        copyToSharedState.writeRows(serializer->getBlobData(), serializer->getSize());
        serializer->reset();
    }
}

static void finalizeFunc(CopyFuncSharedState&) {}

function_set CopyCSVFunction::getFunctionSet() {
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

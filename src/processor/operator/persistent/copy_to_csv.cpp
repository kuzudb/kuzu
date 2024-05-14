#include "processor/operator/persistent/copy_to_csv.h"

#include <fcntl.h>

#include "common/file_system/virtual_file_system.h"
#include "function/cast/vector_cast_functions.h"

namespace kuzu {
namespace processor {

using namespace kuzu::common;
using namespace kuzu::storage;

uint64_t CopyToCSVInfo::getNumFlatVectors() const {
    uint64_t numFlatVectors = 0;
    for (auto flatInfo : isFlat) {
        if (flatInfo) {
            numFlatVectors++;
        }
    }
    return numFlatVectors;
}

void CopyToCSVLocalState::init(CopyToInfo* info, MemoryManager* mm, ResultSet* resultSet) {
    auto copyToCSVInfo = info->constPtrCast<CopyToCSVInfo>();
    serializer = std::make_unique<BufferedSerializer>();
    vectorsToCast.reserve(info->dataPoses.size());
    auto numFlatVectors = copyToCSVInfo->getNumFlatVectors();
    unflatCastDataChunk =
        std::make_unique<DataChunk>(copyToCSVInfo->isFlat.size() - numFlatVectors);
    flatCastDataChunk =
        std::make_unique<DataChunk>(numFlatVectors, DataChunkState::getSingleValueDataChunkState());
    uint64_t numInsertedFlatVector = 0;
    castFuncs.resize(info->dataPoses.size());
    for (auto i = 0u; i < info->dataPoses.size(); i++) {
        auto vectorToCast = resultSet->getValueVector(info->dataPoses[i]);
        castFuncs[i] = function::CastFunction::bindCastFunction("cast",
            vectorToCast->dataType.getLogicalTypeID(), LogicalTypeID::STRING)
                           ->execFunc;
        vectorsToCast.push_back(std::move(vectorToCast));
        auto castVector = std::make_unique<ValueVector>(LogicalTypeID::STRING, mm);
        castVectors.push_back(castVector.get());
        if (copyToCSVInfo->isFlat[i]) {
            flatCastDataChunk->insert(numInsertedFlatVector, std::move(castVector));
            numInsertedFlatVector++;
        } else {
            unflatCastDataChunk->insert(i - numInsertedFlatVector, std::move(castVector));
        }
    }
}

void CopyToCSVLocalState::sink(CopyToSharedState* sharedState, CopyToInfo* info) {
    auto copyToCSVInfo = info->constPtrCast<CopyToCSVInfo>();
    writeRows(copyToCSVInfo);
    if (serializer->getSize() > CopyToCSVConstants::DEFAULT_CSV_FLUSH_SIZE) {
        ku_dynamic_cast<CopyToSharedState*, CopyToCSVSharedState*>(sharedState)
            ->writeRows(serializer->getBlobData(), serializer->getSize());
        serializer->reset();
    }
}

void CopyToCSVLocalState::finalize(CopyToSharedState* sharedState) {
    if (serializer->getSize() > 0) {
        ku_dynamic_cast<CopyToSharedState*, CopyToCSVSharedState*>(sharedState)
            ->writeRows(serializer->getBlobData(), serializer->getSize());
        serializer->reset();
    }
}

void CopyToCSVLocalState::writeString(common::BufferedSerializer* serializer,
    const CopyToCSVInfo* copyToCsvInfo, const uint8_t* strData, uint64_t strLen, bool forceQuote) {
    if (!forceQuote) {
        forceQuote = requireQuotes(copyToCsvInfo, strData, strLen);
    }
    if (forceQuote) {
        bool requiresEscape = false;
        for (auto i = 0u; i < strLen; i++) {
            if (strData[i] == copyToCsvInfo->copyToOption.quoteChar ||
                strData[i] == copyToCsvInfo->copyToOption.escapeChar) {
                requiresEscape = true;
                break;
            }
        }

        if (!requiresEscape) {
            serializer->writeBufferData(copyToCsvInfo->copyToOption.quoteChar);
            serializer->write(strData, strLen);
            serializer->writeBufferData(copyToCsvInfo->copyToOption.quoteChar);
            return;
        }

        std::string strValToWrite = std::string(reinterpret_cast<const char*>(strData), strLen);
        strValToWrite = addEscapes(copyToCsvInfo->copyToOption.escapeChar,
            copyToCsvInfo->copyToOption.escapeChar, strValToWrite);
        if (copyToCsvInfo->copyToOption.escapeChar != copyToCsvInfo->copyToOption.quoteChar) {
            strValToWrite = addEscapes(copyToCsvInfo->copyToOption.quoteChar,
                copyToCsvInfo->copyToOption.escapeChar, strValToWrite);
        }
        serializer->writeBufferData(copyToCsvInfo->copyToOption.quoteChar);
        serializer->writeBufferData(strValToWrite);
        serializer->writeBufferData(copyToCsvInfo->copyToOption.quoteChar);
    } else {
        serializer->write(strData, strLen);
    }
}

bool CopyToCSVLocalState::requireQuotes(const CopyToCSVInfo* copyToCsvInfo, const uint8_t* str,
    uint64_t len) {
    // Check if the string is equal to the null string.
    if (len == strlen(CopyToCSVConstants::DEFAULT_NULL_STR) &&
        memcmp(str, CopyToCSVConstants::DEFAULT_NULL_STR, len) == 0) {
        return true;
    }
    for (auto i = 0u; i < len; i++) {
        if (str[i] == copyToCsvInfo->copyToOption.quoteChar ||
            str[i] == CopyToCSVConstants::DEFAULT_CSV_NEWLINE[0] ||
            str[i] == copyToCsvInfo->copyToOption.delimiter) {
            return true;
        }
    }
    return false;
}

std::string CopyToCSVLocalState::addEscapes(char toEscape, char escape, const std::string& val) {
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

void CopyToCSVLocalState::writeRows(const CopyToCSVInfo* copyToCsvInfo) {
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
                serializer->writeBufferData(copyToCsvInfo->copyToOption.delimiter);
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
            writeString(serializer.get(), copyToCsvInfo, strValue.getData(), strValue.len,
                CopyToCSVConstants::DEFAULT_FORCE_QUOTE ||
                    vectorsToCast[j]->dataType.getLogicalTypeID() == LogicalTypeID::LIST);
        }
        serializer->writeBufferData(CopyToCSVConstants::DEFAULT_CSV_NEWLINE);
    }
}

void CopyToCSVSharedState::init(CopyToInfo* info, main::ClientContext* context) {
    fileInfo =
        context->getVFSUnsafe()->openFile(info->fileName, O_WRONLY | O_CREAT | O_TRUNC, context);
    writeHeader(info);
}

void CopyToCSVSharedState::writeRows(const uint8_t* data, uint64_t size) {
    std::lock_guard<std::mutex> lck(mtx);
    fileInfo->writeFile(data, size, offset);
    offset += size;
}

void CopyToCSVSharedState::writeHeader(CopyToInfo* info) {
    auto copyToCSVInfo = ku_dynamic_cast<CopyToInfo*, CopyToCSVInfo*>(info);
    BufferedSerializer bufferedSerializer;
    if (copyToCSVInfo->copyToOption.hasHeader) {
        for (auto i = 0u; i < info->names.size(); i++) {
            if (i != 0) {
                bufferedSerializer.writeBufferData(copyToCSVInfo->copyToOption.delimiter);
            }
            CopyToCSVLocalState::writeString(&bufferedSerializer, copyToCSVInfo,
                reinterpret_cast<const uint8_t*>(info->names[i].c_str()), info->names[i].length(),
                false /* forceQuote */);
        }
        bufferedSerializer.writeBufferData(CopyToCSVConstants::DEFAULT_CSV_NEWLINE);
        writeRows(bufferedSerializer.getBlobData(), bufferedSerializer.getSize());
    }
}

} // namespace processor
} // namespace kuzu

#include "processor/operator/persistent/copy_to_csv.h"

#include "function/cast/vector_cast_functions.h"

namespace kuzu {
namespace processor {

using namespace kuzu::common;
using namespace kuzu::storage;

uint64_t CopyToCSVInfo::getNumFlatVectors() {
    uint64_t numFlatVectors = 0;
    for (auto flatInfo : isFlat) {
        if (flatInfo) {
            numFlatVectors++;
        }
    }
    return numFlatVectors;
}

void CopyToCSVLocalState::init(CopyToInfo* info, MemoryManager* mm, ResultSet* resultSet) {
    auto csvInfo = reinterpret_cast<CopyToCSVInfo*>(info);
    serializer = std::make_unique<BufferedSerializer>();
    vectorsToCast.reserve(info->dataPoses.size());
    auto numFlatVectors = csvInfo->getNumFlatVectors();
    unflatCastDataChunk = std::make_unique<DataChunk>(csvInfo->isFlat.size() - numFlatVectors);
    flatCastDataChunk =
        std::make_unique<DataChunk>(numFlatVectors, DataChunkState::getSingleValueDataChunkState());
    uint64_t numInsertedFlatVector = 0;
    castFuncs.resize(info->dataPoses.size());
    for (auto i = 0u; i < info->dataPoses.size(); i++) {
        auto vectorToCast = resultSet->getValueVector(info->dataPoses[i]);
        castFuncs[i] = function::CastFunction::bindCastFunction(
            "cast", vectorToCast->dataType.getLogicalTypeID(), LogicalTypeID::STRING)
                           ->execFunc;
        vectorsToCast.push_back(std::move(vectorToCast));
        auto castVector = std::make_unique<ValueVector>(LogicalTypeID::STRING, mm);
        castVectors.push_back(castVector.get());
        if (csvInfo->isFlat[i]) {
            flatCastDataChunk->insert(numInsertedFlatVector, std::move(castVector));
            numInsertedFlatVector++;
        } else {
            unflatCastDataChunk->insert(i - numInsertedFlatVector, std::move(castVector));
        }
    }
}

void CopyToCSVLocalState::sink(CopyToSharedState* sharedState) {
    writeRows();
    if (serializer->getSize() > CopyToCSVConstants::DEFAULT_CSV_FLUSH_SIZE) {
        reinterpret_cast<CopyToCSVSharedState*>(sharedState)
            ->writeRows(serializer->getBlobData(), serializer->getSize());
        serializer->reset();
    }
}

void CopyToCSVLocalState::finalize(CopyToSharedState* sharedState) {
    if (serializer->getSize() > 0) {
        reinterpret_cast<CopyToCSVSharedState*>(sharedState)
            ->writeRows(serializer->getBlobData(), serializer->getSize());
        serializer->reset();
    }
}

bool CopyToCSVLocalState::requireQuotes(const uint8_t* str, uint64_t len) {
    // Check if the string is equal to the null string.
    if (len == strlen(CopyToCSVConstants::DEFAULT_NULL_STR) &&
        memcmp(str, CopyToCSVConstants::DEFAULT_NULL_STR, len) == 0) {
        return true;
    }
    for (auto i = 0u; i < len; i++) {
        if (str[i] == CopyToCSVConstants::DEFAULT_QUOTE[0] ||
            str[i] == CopyToCSVConstants::DEFAULT_CSV_NEWLINE[0] ||
            str[i] == CopyToCSVConstants::DEFAULT_CSV_DELIMITER[0]) {
            return true;
        }
    }
    return false;
}

std::string CopyToCSVLocalState::addEscapes(
    const char* toEscape, const char* escape, const std::string& val) {
    uint64_t i = 0;
    std::string escapedStr = "";
    auto found = val.find(toEscape);

    while (found != std::string::npos) {
        while (i < found) {
            escapedStr += val[i];
            i++;
        }
        escapedStr += escape;
        found = val.find(toEscape, found + strlen(escape));
    }
    while (i < val.length()) {
        escapedStr += val[i];
        i++;
    }
    return escapedStr;
}

void CopyToCSVLocalState::writeString(const uint8_t* strData, uint64_t strLen, bool forceQuote) {
    if (!forceQuote) {
        forceQuote = requireQuotes(strData, strLen);
    }
    if (forceQuote) {
        bool requiresEscape = false;
        for (auto i = 0; i < strLen; i++) {
            if (strData[i] == CopyToCSVConstants::DEFAULT_QUOTE[0] ||
                strData[i] == CopyToCSVConstants::DEFAULT_ESCAPE[0]) {
                requiresEscape = true;
                break;
            }
        }

        if (!requiresEscape) {
            serializer->writeBufferData(std::string(CopyToCSVConstants::DEFAULT_QUOTE));
            serializer->write(strData, strLen);
            serializer->writeBufferData(std::string(CopyToCSVConstants::DEFAULT_QUOTE));
            return;
        }

        std::string strValToWrite = std::string(reinterpret_cast<const char*>(strData), strLen);
        strValToWrite = addEscapes(
            CopyToCSVConstants::DEFAULT_ESCAPE, CopyToCSVConstants::DEFAULT_ESCAPE, strValToWrite);
        if (CopyToCSVConstants::DEFAULT_ESCAPE != CopyToCSVConstants::DEFAULT_QUOTE) {
            strValToWrite = addEscapes(CopyToCSVConstants::DEFAULT_QUOTE,
                CopyToCSVConstants::DEFAULT_ESCAPE, strValToWrite);
        }
        serializer->writeBufferData(std::string(CopyToCSVConstants::DEFAULT_QUOTE));
        serializer->writeBufferData(strValToWrite);
        serializer->writeBufferData(std::string(CopyToCSVConstants::DEFAULT_QUOTE));
    } else {
        serializer->write(strData, strLen);
    }
}

void CopyToCSVLocalState::writeRows() {
    for (auto i = 0u; i < vectorsToCast.size(); i++) {
        std::vector<std::shared_ptr<ValueVector>> vectorToCast = {vectorsToCast[i]};
        castFuncs[i](vectorToCast, *castVectors[i], nullptr);
    }

    uint64_t numRowsToWrite = 1;
    for (auto& vectorToCast : vectorsToCast) {
        if (!vectorToCast->state->isFlat()) {
            numRowsToWrite = vectorToCast->state->selVector->selectedSize;
        }
    }
    for (auto i = 0; i < numRowsToWrite; i++) {
        for (auto j = 0u; j < castVectors.size(); j++) {
            if (j != 0) {
                serializer->writeBufferData(CopyToCSVConstants::DEFAULT_CSV_DELIMITER);
            }
            auto vector = castVectors[j];
            auto pos = vector->state->isFlat() ? vector->state->selVector->selectedPositions[0] :
                                                 vector->state->selVector->selectedPositions[i];
            if (vector->isNull(pos)) {
                // write null value
                serializer->writeBufferData(CopyToCSVConstants::DEFAULT_NULL_STR);
                continue;
            }
            auto strValue = vector->getValue<ku_string_t>(pos);
            // Note: we need blindly add quotes to VAR_LIST.
            writeString(strValue.getData(), strValue.len,
                CopyToCSVConstants::DEFAULT_FORCE_QUOTE ||
                    vectorsToCast[j]->dataType.getLogicalTypeID() == LogicalTypeID::VAR_LIST);
        }
        serializer->writeBufferData(CopyToCSVConstants::DEFAULT_CSV_NEWLINE);
    }
}

void CopyToCSVSharedState::init(CopyToInfo* info, MemoryManager* /*mm*/) {
    fileInfo = FileUtils::openFile(info->fileName, O_WRONLY | O_CREAT | O_TRUNC);
}

void CopyToCSVSharedState::writeRows(const uint8_t* data, uint64_t size) {
    std::lock_guard<std::mutex> lck(mtx);
    FileUtils::writeToFile(fileInfo.get(), data, size, offset);
    offset += size;
}

} // namespace processor
} // namespace kuzu

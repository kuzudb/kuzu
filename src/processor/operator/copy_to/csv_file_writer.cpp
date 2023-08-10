#include "processor/operator/copy_to/csv_file_writer.h"

#include <fstream>

#include "common/constants.h"
#include "common/string_utils.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

void CSVFileWriter::open(const std::string& filePath) {
    fileInfo = FileUtils::openFile(filePath, O_WRONLY | O_CREAT | O_TRUNC);
}

void CSVFileWriter::writeHeader(const std::vector<std::string>& columnNames) {
    if (columnNames.size() == 0) {
        return;
    }
    writeToBuffer(columnNames[0]);
    for (uint64_t i = 1; i < columnNames.size(); i++) {
        writeToBuffer(CopyConstants::DEFAULT_CSV_DELIMITER);
        writeToBuffer(columnNames[i]);
    }
    writeToBuffer(CopyConstants::DEFAULT_CSV_LINE_BREAK);
    flush();
}

void CSVFileWriter::writeValues(std::vector<ValueVector*>& outputVectors) {
    bool hasData = true;
    if (outputVectors.size() == 0) {
        return;
    }
    auto i = 0u;
    for (; i < outputVectors.size() - 1; i++) {
        assert(outputVectors[i]->state->isFlat());
        writeValue(outputVectors[i]);
        writeToBuffer(CopyConstants::DEFAULT_CSV_DELIMITER);
    }
    writeValue(outputVectors[i]);
    writeToBuffer(CopyConstants::DEFAULT_CSV_LINE_BREAK);
    flush();
}

template<typename T>
void CSVFileWriter::writeToBuffer(ValueVector* vector, int64_t pos, bool escapeStringValue) {
    auto value = TypeUtils::toString(vector->getValue<T>(pos));
    if (escapeStringValue) {
        escapeString(value);
    }
    writeToBuffer(value);
}

template<typename T>
void CSVFileWriter::writeListToBuffer(ValueVector* vector, int64_t pos) {
    auto value = TypeUtils::toString(vector->getValue<T>(pos), vector);
    escapeString(value);
    writeToBuffer(value);
}

void CSVFileWriter::escapeString(std::string& value) {
    StringUtils::replaceAll(value, "\"", "\"\"");
    value = "\"" + value + "\"";
}

void CSVFileWriter::writeValue(ValueVector* vector) {
    // vectors are always flat
    auto selPos = vector->state->selVector->selectedPositions[0];
    switch (vector->dataType.getLogicalTypeID()) {
    case LogicalTypeID::BOOL:
        return writeToBuffer<int8_t>(vector, selPos);
    case LogicalTypeID::INT64:
        return writeToBuffer<int64_t>(vector, selPos);
    case LogicalTypeID::INT32:
        return writeToBuffer<int32_t>(vector, selPos);
    case LogicalTypeID::INT16:
        return writeToBuffer<int16_t>(vector, selPos);
    case LogicalTypeID::DOUBLE:
        return writeToBuffer<double>(vector, selPos);
    case LogicalTypeID::FLOAT:
        return writeToBuffer<float>(vector, selPos);
    case LogicalTypeID::DATE:
        return writeToBuffer<date_t>(vector, selPos, true);
    case LogicalTypeID::TIMESTAMP:
        return writeToBuffer<timestamp_t>(vector, selPos, true);
    case LogicalTypeID::INTERVAL:
        return writeToBuffer<interval_t>(vector, selPos, true);
    case LogicalTypeID::STRING:
        return writeToBuffer<ku_string_t>(vector, selPos, true);
    case LogicalTypeID::INTERNAL_ID:
        return writeToBuffer<internalID_t>(vector, selPos, true);
    case LogicalTypeID::VAR_LIST:
    case LogicalTypeID::FIXED_LIST:
        return writeListToBuffer<list_entry_t>(vector, selPos);
    case LogicalTypeID::STRUCT:
        return writeListToBuffer<struct_entry_t>(vector, selPos);
    default: {
        NotImplementedException("CSVFileWriter::writeValue");
    }
    }
}

void CSVFileWriter::flush() {
    const std::string str = buffer.str();
    FileUtils::writeToFile(fileInfo.get(), (uint8_t*)str.data(), str.size(), fileOffset);
    fileOffset += str.size();
    buffer.str("");
}

} // namespace processor
} // namespace kuzu

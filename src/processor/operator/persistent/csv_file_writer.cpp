#include "processor/operator/persistent/csv_file_writer.h"

#include <fcntl.h>

#include "common/constants.h"
#include "common/file_utils.h"
#include "common/string_utils.h"
#include "common/type_utils.h"
#include "common/types/date_t.h"
#include "common/types/internal_id_t.h"
#include "common/types/interval_t.h"
#include "common/types/ku_string.h"
#include "common/types/timestamp_t.h"
#include "common/types/types.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

void CSVFileWriter::openFile() {
    fileInfo = FileUtils::openFile(filePath, O_WRONLY | O_CREAT | O_TRUNC);
}

void CSVFileWriter::init() {
    openFile();
    writeHeader();
}

void CSVFileWriter::writeHeader() {
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

void CSVFileWriter::writeValues(std::vector<common::ValueVector*>& outputVectors) {
    if (outputVectors.size() == 0) {
        return;
    }
    auto i = 0u;
    for (; i < outputVectors.size() - 1; i++) {
        KU_ASSERT(outputVectors[i]->state->isFlat());
        writeValue(outputVectors[i]);
        writeToBuffer(CopyConstants::DEFAULT_CSV_DELIMITER);
    }
    writeValue(outputVectors[i]);
    writeToBuffer(CopyConstants::DEFAULT_CSV_LINE_BREAK);
    flush();
}

template<typename T>
void CSVFileWriter::writeToBuffer(common::ValueVector* vector, bool escapeStringValue) {
    auto selPos = vector->state->selVector->selectedPositions[0];
    auto value = vector->isNull(selPos) ? "" :
                                          TypeUtils::toString(vector->getValue<T>(selPos),
                                              reinterpret_cast<void*>(vector));
    if (escapeStringValue) {
        escapeString(value);
    }
    writeToBuffer(value);
}

void CSVFileWriter::escapeString(std::string& value) {
    StringUtils::replaceAll(value, "\"", "\"\"");
    value = "\"" + value + "\"";
}

void CSVFileWriter::writeValue(common::ValueVector* vector) {
    switch (vector->dataType.getLogicalTypeID()) {
    case LogicalTypeID::BOOL:
        return writeToBuffer<int8_t>(vector);
    case LogicalTypeID::SERIAL:
    case LogicalTypeID::INT64:
        return writeToBuffer<int64_t>(vector);
    case LogicalTypeID::INT32:
        return writeToBuffer<int32_t>(vector);
    case LogicalTypeID::INT16:
        return writeToBuffer<int16_t>(vector);
    case LogicalTypeID::DOUBLE:
        return writeToBuffer<double>(vector);
    case LogicalTypeID::FLOAT:
        return writeToBuffer<float>(vector);
    case LogicalTypeID::DATE:
        return writeToBuffer<date_t>(vector, true /* escapeStringValue */);
    case LogicalTypeID::TIMESTAMP:
        return writeToBuffer<timestamp_t>(vector, true /* escapeStringValue */);
    case LogicalTypeID::INTERVAL:
        return writeToBuffer<interval_t>(vector, true /* escapeStringValue */);
    case LogicalTypeID::STRING:
        return writeToBuffer<ku_string_t>(vector, true /* escapeStringValue */);
    case LogicalTypeID::INTERNAL_ID:
        return writeToBuffer<internalID_t>(vector, true /* escapeStringValue */);
    case LogicalTypeID::VAR_LIST:
    case LogicalTypeID::FIXED_LIST:
        return writeToBuffer<list_entry_t>(vector, true /* escapeStringValue */);
    case LogicalTypeID::STRUCT:
        return writeToBuffer<struct_entry_t>(vector, true /* escapeStringValue */);
    default: {
        KU_UNREACHABLE;
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

#pragma once

#include "common/copier_config/copier_config.h"
#include "common/file_utils.h"
#include "processor/result/result_set.h"

namespace kuzu {
namespace processor {

class CSVFileWriter {
public:
    CSVFileWriter(){};
    void open(const std::string& filePath);
    void writeHeader(const std::vector<std::string>& columnNames);
    void writeValues(std::vector<common::ValueVector*>& outputVectors);

private:
    void escapeString(std::string& value);
    void writeValue(common::ValueVector* vector);
    void flush();

    template<typename T>
    void writeToBuffer(common::ValueVector* vector, int64_t pos, bool escapeStringValue = false);

    template<typename T>
    void writeListToBuffer(common::ValueVector* vector, int64_t pos);

    inline void writeToBuffer(const std::string& value) { buffer << value; }
    inline void writeToBuffer(const char value) { buffer << value; }

    uint64_t fileOffset = 0;
    std::stringstream buffer;
    std::unique_ptr<common::FileInfo> fileInfo;
};

} // namespace processor
} // namespace kuzu

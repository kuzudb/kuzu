#pragma once

#include "processor/operator/copy_to/csv_parquet_writer.h"
#include "processor/result/result_set.h"

namespace kuzu {
namespace processor {

class CSVWriter : public CSVParquetWriter {
public:
    CSVWriter(){};
    void openFile(const std::string& filePath) override;
    void init() override;
    inline void closeFile() override { flush(); }
    void writeValues(std::vector<common::ValueVector*>& outputVectors) override;

private:
    void writeHeader(const std::vector<std::string>& columnNames);
    void escapeString(std::string& value);
    void writeValue(common::ValueVector* vector);
    void flush();

    template<typename T>
    void writeToBuffer(common::ValueVector* vector, bool escapeStringValue = false);

    template<typename T>
    void writeListToBuffer(common::ValueVector* vector);

    inline void writeToBuffer(const std::string& value) { buffer << value; }
    inline void writeToBuffer(const char value) { buffer << value; }

    uint64_t fileOffset = 0;
    std::stringstream buffer;
    std::unique_ptr<common::FileInfo> fileInfo;
};

} // namespace processor
} // namespace kuzu

#pragma once

#include <sstream>

#include "processor/operator/persistent/file_writer.h"

namespace kuzu {
namespace processor {

class CSVFileWriter : public FileWriter {
public:
    using FileWriter::FileWriter;
    void openFile() final;
    void init() final;
    inline void closeFile() final { flush(); }
    void writeValues(std::vector<common::ValueVector*>& outputVectors) final;

private:
    void writeHeader();
    void escapeString(std::string& value);
    void writeValue(common::ValueVector* vector);
    void flush();

    template<typename T>
    void writeToBuffer(common::ValueVector* vector, bool escapeStringValue = false);

    inline void writeToBuffer(const std::string& value) { buffer << value; }
    inline void writeToBuffer(const char value) { buffer << value; }

    uint64_t fileOffset = 0;
    std::stringstream buffer;
    std::unique_ptr<common::FileInfo> fileInfo;
};

} // namespace processor
} // namespace kuzu

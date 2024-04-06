#pragma once

#include "common/types/types.h"
#include "common/vector/value_vector.h"

namespace kuzu {
namespace processor {

class FileWriter {
public:
    FileWriter(std::string filePath, std::vector<std::string> columnNames,
        std::vector<std::unique_ptr<common::LogicalType>> columnTypes)
        : filePath{std::move(filePath)}, columnNames{std::move(columnNames)},
          columnTypes{std::move(columnTypes)} {}
    virtual ~FileWriter() = default;
    virtual void init() = 0;
    virtual void openFile() = 0;
    virtual void closeFile() = 0;
    virtual void writeValues(std::vector<common::ValueVector*>& outputVectors) = 0;

protected:
    std::string filePath;
    std::vector<std::string> columnNames;
    std::vector<std::unique_ptr<common::LogicalType>> columnTypes;
};

} // namespace processor
} // namespace kuzu

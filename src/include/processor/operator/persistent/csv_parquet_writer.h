#pragma once

#include "common/copier_config/copier_config.h"
#include "processor/result/result_set.h"

namespace kuzu {
namespace processor {

class CSVParquetWriter {
public:
    virtual ~CSVParquetWriter(){};
    virtual void openFile(const std::string& filePath) = 0;
    virtual void init() = 0;
    virtual void closeFile() = 0;
    virtual void writeValues(std::vector<common::ValueVector*>& outputVectors) = 0;

    inline void setColumns(const std::vector<std::string>& columnNames,
        const std::vector<common::LogicalType>& columnTypes) {
        this->columnNames = columnNames;
        this->columnTypes = columnTypes;
    }
    inline std::vector<std::string>& getColumnNames() { return columnNames; }
    inline std::vector<common::LogicalType>& getColumnTypes() { return columnTypes; }

private:
    std::vector<std::string> columnNames;
    std::vector<common::LogicalType> columnTypes;
};

} // namespace processor
} // namespace kuzu

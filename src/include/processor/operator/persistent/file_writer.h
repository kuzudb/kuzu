#pragma once

#include "common/copier_config/copier_config.h"
#include "processor/result/result_set.h"

namespace kuzu {
namespace processor {

class FileWriter {
public:
    FileWriter(std::string filePath, std::vector<std::string> columnNames,
        std::vector<common::LogicalType> columnTypes)
        : filePath{filePath}, columnNames{columnNames}, columnTypes{columnTypes} {}
    virtual ~FileWriter(){};
    virtual void init() = 0;
    virtual void openFile() = 0;
    virtual void closeFile() = 0;
    virtual void writeValues(std::vector<common::ValueVector*>& outputVectors) = 0;

protected:
    std::string filePath;
    std::vector<std::string> columnNames;
    std::vector<common::LogicalType> columnTypes;
};

} // namespace processor
} // namespace kuzu

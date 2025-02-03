#pragma once

#include <algorithm>
#include <string>
#include <vector>

#include "main/kuzu.h"

namespace kuzu {
namespace testing {

// Convert an entire CSV dataset directory to some specified output.
// The dataset directory must contain schema and copy files.
class CSVConverter {
public:
    explicit CSVConverter(std::string csvDatasetPath, std::string outputDatasetPath,
        uint64_t bufferPoolSize, std::string outputFileExtension)
        : csvDatasetPath{csvDatasetPath}, outputDatasetPath{outputDatasetPath},
          bufferPoolSize{bufferPoolSize}, fileExtension{outputFileExtension} {}

    void convertCSVDataset();

private:
    void copySchemaFile();
    void createTableInfo(std::string schemaFile);
    void readCopyCommandsFromCSVDataset();
    void createCopyFile();
    void convertCSVFiles();

private:
    struct TableInfo {
        std::string name;
        std::string csvFilePath;
        std::string outputFilePath;
        // get cypher query to convert csv file to output file
        virtual std::string getConverterQuery() const = 0;
    };

    struct NodeTableInfo final : public TableInfo {
        std::string primaryKey;
        // get converter query for node table
        std::string getConverterQuery() const override;
    };

    struct RelTableInfo final : public TableInfo {
        std::shared_ptr<NodeTableInfo> fromTable;
        std::shared_ptr<NodeTableInfo> toTable;
        // get converter query for rel table
        std::string getConverterQuery() const override;
    };

private:
    std::string csvDatasetPath;
    std::string outputDatasetPath;
    std::vector<std::shared_ptr<TableInfo>> tables;
    std::unordered_map<std::string, std::shared_ptr<TableInfo>> tableNameMap;
    uint64_t bufferPoolSize;
    std::string fileExtension;
    std::unique_ptr<main::SystemConfig> systemConfig;
    std::unique_ptr<main::Database> tempDb;
    std::unique_ptr<main::Connection> tempConn;
};

} // namespace testing
} // namespace kuzu

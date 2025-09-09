#pragma once

#include <algorithm>
#include <string>
#include <utility>
#include <vector>

#include "main/kuzu.h"

namespace kuzu {
namespace testing {

// Convert an entire CSV dataset directory to some specified output.
// The dataset directory must contain schema and copy files.
class CSVConverter {
public:
    explicit CSVConverter(std::string csvDatasetPath, std::string outputDatasetPath,
        std::optional<uint64_t> bufferPoolSize, std::string outputFileExtension)
        : csvDatasetPath{std::move(csvDatasetPath)},
          outputDatasetPath{std::move(outputDatasetPath)}, bufferPoolSize{bufferPoolSize},
          fileExtension{std::move(outputFileExtension)} {}

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
        virtual std::string getConverterQuery(main::ClientContext* context) const = 0;
        virtual ~TableInfo() = default;
    };

    struct NodeTableInfo final : TableInfo {
        std::string primaryKey;
        // get converter query for node table
        std::string getConverterQuery(main::ClientContext* context) const override;
    };

    struct RelTableInfo final : TableInfo {
        std::shared_ptr<NodeTableInfo> fromTable;
        std::shared_ptr<NodeTableInfo> toTable;
        // get converter query for rel table
        std::string getConverterQuery(main::ClientContext* context) const override;
    };

private:
    std::string csvDatasetPath;
    std::string outputDatasetPath;
    std::vector<std::shared_ptr<TableInfo>> tables;
    std::unordered_map<std::string, std::shared_ptr<TableInfo>> tableNameMap;
    std::optional<uint64_t> bufferPoolSize;
    std::string fileExtension;
    std::unique_ptr<main::SystemConfig> systemConfig;
    std::unique_ptr<main::Database> tempDb;
    std::unique_ptr<main::Connection> tempConn;
};

} // namespace testing
} // namespace kuzu

#pragma once

#include <string>
#include <vector>

#include "main/kuzu.h"

namespace kuzu {
namespace testing {

// Insert a dataset row by row instead of batch insert (copy)
class InsertDatasetByRow {
public:
    InsertDatasetByRow(std::string datasetPath, main::Connection& connection)
        : datasetPath{datasetPath}, connection{connection} {}
    void init();
    void run();

private:
    struct RelConnection {
        std::string name;
        std::string property;
        std::string type;
    };

    struct TableInfo {
        std::string name;
        std::string filePath;
        std::vector<std::pair<std::string, std::string>> properties;
        TableInfo(std::string name, std::string filePath,
            std::vector<std::pair<std::string, std::string>> properties)
            : name{std::move(name)}, filePath{std::move(filePath)},
              properties{std::move(properties)} {}
        virtual std::string getLoadFromQuery() const = 0;
        virtual ~TableInfo() = default;

        std::string getHeaderForLoad() const;
        std::string getBodyForLoad() const;
    };

    struct NodeTableInfo final : public TableInfo {
        NodeTableInfo(std::string name, std::string filePath,
            std::vector<std::pair<std::string, std::string>> properties)
            : TableInfo{std::move(name), std::move(filePath), std::move(properties)} {}
        std::string getLoadFromQuery() const override;
    };

    struct RelTableInfo final : public TableInfo {
        RelConnection from;
        RelConnection to;
        RelTableInfo(std::string name, std::string filePath,
            std::vector<std::pair<std::string, std::string>> properties, RelConnection from,
            RelConnection to)
            : TableInfo{std::move(name), std::move(filePath), std::move(properties)},
              from{std::move(from)}, to{std::move(to)} {}
        std::string getLoadFromQuery() const override;
    };

private:
    std::string datasetPath;
    main::Connection& connection;
    std::vector<std::unique_ptr<TableInfo>> tables;
};

} // namespace testing
} // namespace kuzu

#pragma once

#include "common/copier_config/csv_reader_config.h"
#include "common/copier_config/reader_config.h"
#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalExportDatabase : public LogicalOperator {
public:
    explicit LogicalExportDatabase(std::string filePath, std::vector<std::string> tableNames,
        std::vector<std::string> tableCyphers, std::vector<std::string> macroCyphers,
        std::vector<std::shared_ptr<LogicalOperator>> plans)
        : LogicalOperator{LogicalOperatorType::EXPORT_DATABASE}, filePath{std::move(filePath)},
          tableNames(std::move(tableNames)), tableCyphers{std::move(tableCyphers)},
          macroCyphers{std::move(macroCyphers)} {
        children = std::move(plans);
    }

    inline std::string getExpressionsForPrinting() const override { return std::string{}; }

    void computeFactorizedSchema() override;
    void computeFlatSchema() override;

    inline std::string getFilePath() const { return filePath; }
    inline std::vector<std::string> getTableNames() const { return tableNames; }
    inline std::vector<std::string> getTableCyphers() const { return tableCyphers; }
    inline std::vector<std::string> getMacroCyphers() const { return macroCyphers; }

    inline std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalExportDatabase>(filePath, std::move(tableNames),
            std::move(tableCyphers), std::move(macroCyphers), std::move(children));
    }

private:
    std::string filePath;
    std::vector<std::string> tableNames;
    std::vector<std::string> tableCyphers;
    std::vector<std::string> macroCyphers;
};

} // namespace planner
} // namespace kuzu

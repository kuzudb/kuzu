#pragma once

#include "binder/query/bound_regular_query.h"
#include "common/copier_config/copier_config.h"

namespace kuzu {
namespace binder {

class BoundCopyTo : public BoundStatement {
public:
    BoundCopyTo(std::string filePath, common::FileType fileType,
        std::vector<std::string> columnNames,
        std::vector<std::unique_ptr<common::LogicalType>> columnTypes,
        std::unique_ptr<BoundRegularQuery> regularQuery,
        std::unique_ptr<common::CSVOption> csvOption)
        : BoundStatement{common::StatementType::COPY_TO, BoundStatementResult::createEmptyResult()},
          filePath{std::move(filePath)}, fileType{fileType}, columnNames{std::move(columnNames)},
          columnTypes{std::move(columnTypes)},
          regularQuery{std::move(regularQuery)}, csvOption{std::move(csvOption)} {}

    inline std::string getFilePath() const { return filePath; }
    inline common::FileType getFileType() const { return fileType; }
    inline std::vector<std::string> getColumnNames() const { return columnNames; }
    inline const std::vector<std::unique_ptr<common::LogicalType>>& getColumnTypesRef() const {
        return columnTypes;
    }

    inline BoundRegularQuery* getRegularQuery() const { return regularQuery.get(); }
    inline common::CSVOption* getCopyOption() const { return csvOption.get(); }

private:
    std::string filePath;
    common::FileType fileType;
    std::vector<std::string> columnNames;
    std::vector<std::unique_ptr<common::LogicalType>> columnTypes;
    std::unique_ptr<BoundRegularQuery> regularQuery;
    std::unique_ptr<common::CSVOption> csvOption;
};

} // namespace binder
} // namespace kuzu

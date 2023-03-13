#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include "binder/bound_statement.h"
#include "catalog/catalog_structs.h"
#include "common/copier_config/copier_config.h"

namespace kuzu {
namespace binder {

class BoundCopy : public BoundStatement {
public:
    BoundCopy(std::shared_ptr<Expression> filePaths, common::CSVReaderConfig csvReaderConfig,
        common::table_id_t tableID, std::string tableName)
        : BoundStatement{common::StatementType::COPY_CSV,
              BoundStatementResult::createSingleStringColumnResult()},
          filePaths{std::move(filePaths)}, csvReaderConfig{std::move(csvReaderConfig)},
          tableID{tableID}, tableName{std::move(tableName)} {}

    inline std::shared_ptr<Expression> getFilePaths() const { return filePaths; }

    inline common::CSVReaderConfig getCSVReaderConfig() const { return csvReaderConfig; }

    inline common::table_id_t getTableID() const { return tableID; }

    inline std::string getTableName() const { return tableName; }

private:
    std::shared_ptr<Expression> filePaths;
    common::CSVReaderConfig csvReaderConfig;
    common::table_id_t tableID;
    std::string tableName;
};

} // namespace binder
} // namespace kuzu

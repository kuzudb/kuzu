#pragma once

#include "binder/expression/expression.h"
#include "catalog/table_schema.h"
#include "common/copier_config/copier_config.h"

namespace kuzu {
namespace binder {

struct BoundFileScanInfo {
    std::unique_ptr<common::ReaderConfig> readerConfig;
    binder::expression_vector columns;
    std::shared_ptr<Expression> offset;

    // TODO: remove the following field
    common::TableType tableType;

    BoundFileScanInfo(std::unique_ptr<common::ReaderConfig> readerConfig,
        binder::expression_vector columns, std::shared_ptr<Expression> offset,
        common::TableType tableType)
        : readerConfig{std::move(readerConfig)}, columns{std::move(columns)},
          offset{std::move(offset)}, tableType{tableType} {}
    BoundFileScanInfo(const BoundFileScanInfo& other)
        : readerConfig{other.readerConfig->copy()}, columns{other.columns}, offset{other.offset},
          tableType{other.tableType} {}

    inline std::unique_ptr<BoundFileScanInfo> copy() const {
        return std::make_unique<BoundFileScanInfo>(*this);
    }
};

} // namespace binder
} // namespace kuzu

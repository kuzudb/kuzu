#pragma once

#include "binder/expression/expression.h"
#include "common/copier_config/copier_config.h"
#include "common/enums/table_type.h"

namespace kuzu {
namespace binder {

struct BoundFileScanInfo {
    std::unique_ptr<common::ReaderConfig> readerConfig;
    binder::expression_vector columns;
    std::shared_ptr<Expression> internalID;

    // TODO: remove the following field
    common::TableType tableType;

    BoundFileScanInfo(std::unique_ptr<common::ReaderConfig> readerConfig,
        binder::expression_vector columns, std::shared_ptr<Expression> internalID,
        common::TableType tableType)
        : readerConfig{std::move(readerConfig)}, columns{std::move(columns)},
          internalID{std::move(internalID)}, tableType{tableType} {}
    BoundFileScanInfo(const BoundFileScanInfo& other)
        : readerConfig{other.readerConfig->copy()}, columns{other.columns},
          internalID{other.internalID}, tableType{other.tableType} {}

    inline std::unique_ptr<BoundFileScanInfo> copy() const {
        return std::make_unique<BoundFileScanInfo>(*this);
    }
};

} // namespace binder
} // namespace kuzu

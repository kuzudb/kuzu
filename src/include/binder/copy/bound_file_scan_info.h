#pragma once

#include "binder/expression/expression.h"
#include "catalog/table_schema.h"
#include "common/copier_config/copier_config.h"

namespace kuzu {
namespace binder {

struct BoundFileScanInfo {
    std::unique_ptr<common::CopyDescription> copyDesc;
    binder::expression_vector columns;
    std::shared_ptr<Expression> offset;

    // TODO: remove the following field
    catalog::TableSchema* tableSchema;
    bool containsSerial;

    BoundFileScanInfo(std::unique_ptr<common::CopyDescription> copyDesc,
        binder::expression_vector columns, std::shared_ptr<Expression> offset,
        catalog::TableSchema* tableSchema, bool containsSerial)
        : copyDesc{std::move(copyDesc)}, columns{std::move(columns)}, offset{std::move(offset)},
          tableSchema{tableSchema}, containsSerial{containsSerial} {}
    BoundFileScanInfo(const BoundFileScanInfo& other)
        : copyDesc{other.copyDesc->copy()}, columns{other.columns}, offset{other.offset},
          tableSchema{other.tableSchema}, containsSerial{other.containsSerial} {}

    inline std::unique_ptr<BoundFileScanInfo> copy() const {
        return std::make_unique<BoundFileScanInfo>(*this);
    }
};

} // namespace binder
} // namespace kuzu

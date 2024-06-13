#pragma once

#include "common/copier_config/reader_config.h"
#include "common/types/types.h"
#include "main/client_context.h"

namespace kuzu {
namespace common {
class FileSystem;
}

namespace function {

struct TableFuncBindData {
    std::vector<common::LogicalType> columnTypes;
    std::vector<std::string> columnNames;

    TableFuncBindData() = default;
    TableFuncBindData(std::vector<common::LogicalType> columnTypes,
        std::vector<std::string> columnNames)
        : columnTypes{std::move(columnTypes)}, columnNames{std::move(columnNames)} {}
    TableFuncBindData(const TableFuncBindData& other)
        : columnTypes{common::LogicalType::copy(other.columnTypes)},
          columnNames{other.columnNames} {}

    virtual ~TableFuncBindData() = default;

    virtual std::unique_ptr<TableFuncBindData> copy() const = 0;

    template<class TARGET>
    const TARGET* constPtrCast() const {
        return common::ku_dynamic_cast<const TableFuncBindData*, const TARGET*>(this);
    }
};

struct ScanBindData : public TableFuncBindData {
    common::ReaderConfig config;
    main::ClientContext* context;

    ScanBindData(std::vector<common::LogicalType> columnTypes, std::vector<std::string> columnNames,
        common::ReaderConfig config, main::ClientContext* context)
        : TableFuncBindData{std::move(columnTypes), std::move(columnNames)},
          config{std::move(config)}, context{context} {}
    ScanBindData(const ScanBindData& other)
        : TableFuncBindData{other}, config{other.config.copy()}, context{other.context} {}

    inline std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<ScanBindData>(*this);
    }
};

} // namespace function
} // namespace kuzu

#pragma once

#include "common/copier_config/reader_config.h"
#include "common/types/types.h"
#include "main/client_context.h"
#include "storage/buffer_manager/memory_manager.h"

namespace kuzu {
namespace common {
class FileSystem;
}

namespace function {

struct TableFuncBindData {
    common::logical_types_t columnTypes;
    std::vector<std::string> columnNames;

    TableFuncBindData() = default;
    TableFuncBindData(common::logical_types_t columnTypes, std::vector<std::string> columnNames)
        : columnTypes{std::move(columnTypes)}, columnNames{std::move(columnNames)} {}
    TableFuncBindData(const TableFuncBindData& other)
        : columnTypes{common::LogicalType::copy(other.columnTypes)}, columnNames{
                                                                         other.columnNames} {}

    virtual ~TableFuncBindData() = default;

    virtual std::unique_ptr<TableFuncBindData> copy() const = 0;
};

struct ScanBindData : public TableFuncBindData {
    storage::MemoryManager* mm;
    common::ReaderConfig config;
    common::VirtualFileSystem* vfs;
    main::ClientContext* context;

    ScanBindData(common::logical_types_t columnTypes, std::vector<std::string> columnNames,
        storage::MemoryManager* mm, common::ReaderConfig config, common::VirtualFileSystem* vfs,
        main::ClientContext* context)
        : TableFuncBindData{std::move(columnTypes), std::move(columnNames)}, mm{mm},
          config{std::move(config)}, vfs{vfs}, context{context} {}
    ScanBindData(const ScanBindData& other)
        : TableFuncBindData{other}, mm{other.mm}, config{other.config.copy()}, vfs{other.vfs},
          context{other.context} {}

    inline std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<ScanBindData>(*this);
    }
};

} // namespace function
} // namespace kuzu

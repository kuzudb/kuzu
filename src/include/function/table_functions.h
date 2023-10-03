#pragma once

#include <functional>
#include <memory>
#include <string>

#include "common/types/internal_id_t.h"

namespace kuzu {
namespace catalog {
class CatalogContent;
} // namespace catalog
namespace common {
class ValueVector;
}
namespace main {
class ClientContext;
}

namespace function {

struct TableFuncBindData;
struct TableFuncBindInput;

using table_func_t = std::function<void(std::pair<common::offset_t, common::offset_t>,
    TableFuncBindData*, std::vector<common::ValueVector*>)>;
using table_bind_func_t = std::function<std::unique_ptr<TableFuncBindData>(
    main::ClientContext* context, TableFuncBindInput input, catalog::CatalogContent* catalog)>;

struct TableFunctionDefinition {
    std::string name;
    table_func_t tableFunc;
    table_bind_func_t bindFunc;

    TableFunctionDefinition(std::string name, table_func_t tableFunc, table_bind_func_t bindFunc)
        : name{std::move(name)}, tableFunc{std::move(tableFunc)}, bindFunc{std::move(bindFunc)} {}
};

} // namespace function
} // namespace kuzu

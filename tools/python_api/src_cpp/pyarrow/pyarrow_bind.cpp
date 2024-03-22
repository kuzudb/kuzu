#include "pyarrow/pyarrow_bind.h"

#include "cached_import/py_cached_import.h"
#include "common/arrow/arrow.h"
#include "common/arrow/arrow_converter.h"

namespace kuzu {

std::shared_ptr<ArrowSchemaWrapper> Pyarrow::bind(py::handle tableToBind,
    std::vector<common::LogicalType>& returnTypes, std::vector<std::string>& names) {

    std::shared_ptr<ArrowSchemaWrapper> schema = std::make_shared<ArrowSchemaWrapper>();
    auto pyschema = tableToBind.attr("schema");
    auto exportSchemaToC = pyschema.attr("_export_to_c");
    exportSchemaToC(reinterpret_cast<uint64_t>(schema.get()));

    for (int64_t i = 0; i < schema->n_children; i++) {
        ArrowSchema* child = schema->children[i];
        names.emplace_back(child->name);
        returnTypes.push_back(common::ArrowConverter::fromArrowSchema(child));
    }

    return schema;
}

} // namespace kuzu

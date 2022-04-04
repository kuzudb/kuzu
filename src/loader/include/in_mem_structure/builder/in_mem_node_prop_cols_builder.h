#pragma once

#include "src/common/include/task_system/task_scheduler.h"
#include "src/loader/include/in_mem_structure/builder/in_mem_structures_builder.h"
#include "src/loader/include/in_mem_structure/column_utils.h"
#include "src/loader/include/in_mem_structure/in_mem_pages.h"

namespace graphflow {
namespace loader {

class InMemNodePropertyColumnsBuilder : public InMemStructuresBuilder, public ColumnUtils {

    using overflow_pages_vec_t = vector<unique_ptr<InMemOverflowPages>>;
    using property_pages_vec_t = vector<unique_ptr<InMemPropertyPages>>;

public:
    // Initialize the builder and construct relevant propertyColumns.
    InMemNodePropertyColumnsBuilder(NodeLabelDescription& description, TaskScheduler& taskScheduler,
        const Catalog& catalog, string outputDirectory);

    void setProperty(node_offset_t nodeOffset, const uint32_t& propertyIdx, const uint8_t* val,
        const DataTypeID& type);

    void setStringProperty(node_offset_t nodeOffset, const uint32_t& propertyIdx,
        const char* originalString, PageByteCursor& cursor);
    void setListProperty(node_offset_t nodeOffset, const uint32_t& propertyIdx,
        const Literal& listVal, PageByteCursor& cursor);

    void saveToFile(LoaderProgressBar* progressBar) override;

private:
    void buildInMemPropertyColumns();

private:
    NodeLabelDescription& description;

    overflow_pages_vec_t propertyColumnOverflowPages;
    property_pages_vec_t propertyColumns;
};

} // namespace loader
} // namespace graphflow

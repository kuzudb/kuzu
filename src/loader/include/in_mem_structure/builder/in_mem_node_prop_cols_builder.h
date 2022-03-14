#pragma once

#include "src/common/include/task_system/task_scheduler.h"
#include "src/loader/include/in_mem_structure/builder/in_mem_structures_builder.h"
#include "src/loader/include/in_mem_structure/column_utils.h"
#include "src/loader/include/in_mem_structure/in_mem_pages.h"

namespace graphflow {
namespace loader {

class InMemNodePropertyColumnsBuilder : public InMemStructuresBuilder, public ColumnUtils {

    typedef vector<unique_ptr<InMemStringOverflowPages>> propertyIdxStringOverflow_t;
    typedef vector<unique_ptr<InMemPropertyPages>> propertyIdxPropertyColumn_t;

public:
    // Initialize the builder and construct relevant propertyColumns.
    InMemNodePropertyColumnsBuilder(NodeLabelDescription& description, TaskScheduler& taskScheduler,
        const Graph& graph, string outputDirectory);

    void setProperty(node_offset_t nodeOffset, const uint32_t& propertyIdx, const uint8_t* val,
        const DataType& type);

    void setStringProperty(node_offset_t nodeOffset, const uint32_t& propertyIdx,
        const char* originalString, PageByteCursor& cursor);

    void saveToFile(LoaderProgressBar* progressBar) override;

private:
    void buildInMemPropertyColumns();

private:
    NodeLabelDescription& description;

    propertyIdxStringOverflow_t propertyIdxStringOverflowPages{};
    propertyIdxPropertyColumn_t propertyIdxPropertyColumn{};
};

} // namespace loader
} // namespace graphflow

#pragma once

#include "src/processor/include/physical_plan/operator/copy_csv/in_mem_builder/in_mem_structures_builder.h"
#include "src/storage/index/include/in_mem_hash_index.h"

namespace graphflow {
namespace processor {

class InMemNodeBuilder : public InMemStructuresBuilder {

public:
    InMemNodeBuilder(const CSVDescription& csvDescription, string outputDirectory,
        TaskScheduler& taskScheduler, const Catalog& catalog);

    ~InMemNodeBuilder() override = default;

    uint64_t load();
    void saveToFile() override;

private:
    void initializeColumnsAndList();
    vector<string> countLinesPerBlockAndParseUnstrPropertyNames(uint64_t numStructuredProperties);
    void populateColumnsAndCountUnstrPropertyListSizes();
    void calcUnstrListsHeadersAndMetadata();
    void populateUnstrPropertyLists();

    static void calcLengthOfUnstrPropertyLists(
        CSVReader& reader, node_offset_t nodeOffset, InMemUnstructuredLists* unstrPropertyLists);
    static void putUnstrPropsOfALineToLists(CSVReader& reader, node_offset_t nodeOffset,
        PageByteCursor& stringOvfPagesCursor,
        unordered_map<string, uint64_t>& unstrPropertiesNameToIdMap,
        InMemUnstructuredLists* unstrPropertyLists);

    static void putPropsOfLineIntoColumns(vector<unique_ptr<InMemColumn>>& columns,
        const vector<Property>& properties, vector<PageByteCursor>& overflowCursors,
        CSVReader& reader, uint64_t nodeOffset);
    template<DataTypeID DT>
    static void addIDsToIndex(InMemColumn* column, InMemHashIndex* hashIndex,
        node_offset_t startOffset, uint64_t numValues);
    static void populateIDIndex(InMemColumn* column, InMemHashIndex* IDIndex,
        node_offset_t startOffset, uint64_t numValues);

    // Concurrent tasks.
    static void populateColumnsAndCountUnstrPropertyListSizesTask(uint64_t IDColumnIdx,
        uint64_t blockId, uint64_t offsetStart, InMemHashIndex* IDIndex, InMemNodeBuilder* builder);
    static void populateUnstrPropertyListsTask(
        uint64_t blockId, node_offset_t nodeOffsetStart, InMemNodeBuilder* builder);

private:
    NodeLabel* nodeLabel;
    uint64_t numNodes;
    vector<unique_ptr<InMemColumn>> structuredColumns;
    unique_ptr<InMemUnstructuredLists> unstrPropertyLists;
};

} // namespace processor
} // namespace graphflow

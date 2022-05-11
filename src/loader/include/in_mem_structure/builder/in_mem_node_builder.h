#pragma once

#include "src/loader/include/in_mem_structure/builder/in_mem_structures_builder.h"

namespace graphflow {
namespace loader {

class InMemNodeBuilder : public InMemStructuresBuilder {

public:
    InMemNodeBuilder(label_t nodeLabel, const NodeFileDescription& fileDescription,
        string outputDirectory, TaskScheduler& taskScheduler, Catalog& catalog,
        LoaderProgressBar* progressBar);

    ~InMemNodeBuilder() override = default;

    unique_ptr<NodeIDMap> load();

    void saveToFile() override;

private:
    void addLabelToCatalogAndCountLines();
    void initializeColumnsAndList();
    vector<unordered_set<string>> countLinesPerBlockAndParseUnstrPropertyNames(
        uint64_t numStructuredProperties);
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
        NodeIDMap* nodeIDMap, const vector<Property>& properties,
        vector<PageByteCursor>& overflowCursors, CSVReader& reader, uint64_t nodeOffset);

    // Concurrent tasks.
    static void populateColumnsAndCountUnstrPropertyListSizesTask(
        uint64_t blockId, uint64_t offsetStart, InMemNodeBuilder* builder);
    static void countLinesAndParseUnstrPropertyNamesInBlockTask(const string& fName,
        uint32_t numStructuredProperties, unordered_set<string>* unstrPropertyNameSet,
        uint32_t blockId, InMemNodeBuilder* builder);
    static void populateUnstrPropertyListsTask(
        uint64_t blockId, node_offset_t offsetStart, InMemNodeBuilder* builder);

private:
    vector<uint64_t> numLinesPerBlock;
    DataType IDType;
    unique_ptr<NodeIDMap> nodeIDMap;

    vector<unique_ptr<InMemColumn>> structuredColumns;
    unique_ptr<InMemUnstructuredLists> unstrPropertyLists;
};

} // namespace loader
} // namespace graphflow

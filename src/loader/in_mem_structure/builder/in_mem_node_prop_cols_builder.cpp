#include "src/loader/include/in_mem_structure/builder/in_mem_node_prop_cols_builder.h"

#include "src/loader/include/in_mem_structure/in_mem_pages.h"
#include "src/loader/include/loader_task.h"

namespace graphflow {
namespace loader {

InMemNodePropertyColumnsBuilder::InMemNodePropertyColumnsBuilder(NodeLabelDescription& description,
    TaskScheduler& taskScheduler, const Graph& graph, string outputDirectory)
    : InMemStructuresBuilder(taskScheduler, graph, move(outputDirectory)),
      description(description) {
    buildInMemPropertyColumns();
}

void InMemNodePropertyColumnsBuilder::buildInMemPropertyColumns() {
    logger->debug("Creating InMemProperty Columns.");
    propertyColumns.resize(description.properties.size());
    propertyColumnOverflowPages.resize(description.properties.size());
    for (auto& property : description.properties) {
        auto fName = NodesStore::getNodePropertyColumnFName(
            outputDirectory, description.label, property.name);
        auto numPages = calcNumPagesInColumn(TypeUtils::getDataTypeSize(property.dataType),
            graph.getNumNodesPerLabel()[description.label]);
        propertyColumns[property.id] = make_unique<InMemPropertyPages>(
            fName, TypeUtils::getDataTypeSize(property.dataType), numPages);
        if (STRING == property.dataType || LIST == property.dataType) {
            propertyColumnOverflowPages[property.id] =
                make_unique<InMemOverflowPages>(OverflowPages::getOverflowPagesFName(fName));
        }
    }
    logger->debug("Done creating InMemProperty Columns.");
}

void InMemNodePropertyColumnsBuilder::setProperty(node_offset_t nodeOffset,
    const uint32_t& propertyIdx, const uint8_t* val, const DataType& type) {
    auto cursor = calcPageElementCursor(TypeUtils::getDataTypeSize(type), nodeOffset);
    propertyColumns[propertyIdx]->set(cursor, val);
}

void InMemNodePropertyColumnsBuilder::setStringProperty(node_offset_t nodeOffset,
    const uint32_t& propertyIdx, const char* originalString, PageByteCursor& cursor) {
    auto gfString = propertyColumnOverflowPages[propertyIdx]->addString(originalString, cursor);
    setProperty(nodeOffset, propertyIdx, reinterpret_cast<uint8_t*>(&gfString), STRING);
}

void InMemNodePropertyColumnsBuilder::setListProperty(node_offset_t nodeOffset,
    const uint32_t& propertyIdx, const Literal& listVal, PageByteCursor& cursor) {
    assert(listVal.dataType == LIST);
    gf_list_t gfList = propertyColumnOverflowPages[propertyIdx]->addList(listVal, cursor);
    setProperty(nodeOffset, propertyIdx, reinterpret_cast<uint8_t*>(&gfList), LIST);
}

void InMemNodePropertyColumnsBuilder::saveToFile(LoaderProgressBar* progressBar) {
    logger->debug("Writing Node Property Columns to disk.");
    uint64_t numTasks = numProgressBarTasksForSavingPropertiesToDisk(description.properties);
    if (numTasks > 0) {
        progressBar->addAndStartNewJob("Saving properties to disk for node: " +
                                           graph.getCatalog().getNodeLabelName(description.label),
            numTasks);
    }
    for (auto& property : description.properties) {
        taskScheduler.scheduleTask(LoaderTaskFactory::createLoaderTask(
            [&](InMemPropertyPages* x, LoaderProgressBar* progressBar) {
                x->saveToFile();
                progressBar->incrementTaskFinished();
            },
            reinterpret_cast<InMemPropertyPages*>(propertyColumns[property.id].get()),
            progressBar));
        if (STRING == property.dataType || LIST == property.dataType) {
            taskScheduler.scheduleTask(LoaderTaskFactory::createLoaderTask(
                [&](InMemOverflowPages* x, LoaderProgressBar* progressBar) {
                    x->saveToFile();
                    progressBar->incrementTaskFinished();
                },
                (propertyColumnOverflowPages)[property.id].get(), progressBar));
        }
    }
    taskScheduler.waitAllTasksToCompleteOrError();
    logger->debug("Done writing Node Property Columns to disk.");
}

} // namespace loader
} // namespace graphflow

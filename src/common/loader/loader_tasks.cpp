#include "src/common/loader/include/loader_tasks.h"

namespace graphflow {
namespace common {

void LoaderTasks::fileBlockLinesCounterTask(string fname, char tokenSeparator,
    vector<vector<uint64_t>> *numLinesPerBlock, gfLabel_t label, uint32_t blockId,
    shared_ptr<spdlog::logger> logger) {
    logger->debug("start {0} {1}", fname, blockId);
    CSVReader reader(fname, tokenSeparator, blockId);
    (*numLinesPerBlock)[label][blockId] = 0ull;
    while (reader.hasMore()) {
        (*numLinesPerBlock)[label][blockId]++;
        reader.skipLine();
    }
    logger->debug("end   {0} {1} {2}", fname, blockId, (*numLinesPerBlock)[label][blockId]);
}

void LoaderTasks::NodePropertyReaderTask(string fname, char tokenSeparator,
    vector<Property> &propertyMap, uint32_t blockId, gfNodeOffset_t offset,
    vector<unique_ptr<InMemoryColumnBase>> *inMemoryColumns, shared_ptr<spdlog::logger> logger) {
    logger->debug("start {0} {1}", fname, blockId);
    CSVReader reader(fname, tokenSeparator, blockId);
    if (0 == blockId) {
        reader.skipLine(); // skip header line
    }
    while (reader.hasMore()) {
        auto nodeID = reader.getNodeID();
        for (auto i = 1u; i < propertyMap.size(); i++) {
            switch (propertyMap[i].dataType) {
            case INT: {
                auto intVal = reader.skipTokenIfNULL() ? NULL_GFINT : reader.getInteger();
                auto a = (InMemoryColumn<gfInt_t> *)(*inMemoryColumns)[i].get();
                a->set(intVal, offset);
                break;
            }
            case DOUBLE: {
                auto doubleVal = reader.skipTokenIfNULL() ? NULL_GFDOUBLE : reader.getDouble();
                ((InMemoryColumn<gfDouble_t> *)(*inMemoryColumns)[i].get())->set(doubleVal, offset);
                break;
            }
            case BOOLEAN: {
                auto boolVal = reader.skipTokenIfNULL() ? NULL_GFBOOL : reader.getBoolean();
                ((InMemoryColumn<gfBool_t> *)(*inMemoryColumns)[i].get())->set(boolVal, offset);
                break;
            }
            default:
                if (!reader.skipTokenIfNULL()) {
                    reader.skipToken();
                }
            }
        }
        offset++;
    }
    logger->debug("end   {0} {1}", fname, blockId);
}

void LoaderTasks::ColumnSerializer(
    const string fname, InMemoryColumnBase *rawColumn, shared_ptr<spdlog::logger> logger) {
    logger->debug("writing {0}", fname);
    int f = open(fname.c_str(), O_WRONLY | O_CREAT, 0666);
    if (-1 == f) {
        throw invalid_argument("Cannot open the file to write: " + fname);
    }
    if (rawColumn->size != write(f, rawColumn->data.get(), rawColumn->size)) {
        throw invalid_argument("Error writing to file: " + fname);
    }
}

} // namespace common
} // namespace graphflow

#pragma once

#include "common/copier_config/rdf_config.h"
#include "common/data_chunk/data_chunk.h"
#include "serd.h"

namespace kuzu {
namespace processor {

class RDFReader {
public:
    explicit RDFReader(std::string filePath, std::unique_ptr<common::RdfReaderConfig> config);

    ~RDFReader();

    common::offset_t read(common::DataChunk* dataChunkToRead);
    common::offset_t countLine();

private:
    static SerdStatus errorHandle(void* handle, const SerdError* error);
    static SerdStatus readerStatementSink(void* handle, SerdStatementFlags flags,
        const SerdNode* graph, const SerdNode* subject, const SerdNode* predicate,
        const SerdNode* object, const SerdNode* object_datatype, const SerdNode* object_lang);
    static SerdStatus prefixSink(void* handle, const SerdNode* name, const SerdNode* uri);

    static SerdStatus counterStatementSink(void* handle, SerdStatementFlags flags,
        const SerdNode* graph, const SerdNode* subject, const SerdNode* predicate,
        const SerdNode* object, const SerdNode* object_datatype, const SerdNode* object_lang);

private:
    const std::string filePath;
    std::unique_ptr<common::RdfReaderConfig> config;

    FILE* fp;
    SerdReader* reader;
    SerdReader* counter;

    // TODO(Xiyang): use prefix to expand CURIE.
    const char* currentPrefix;
    common::offset_t rowOffset;
    common::offset_t vectorSize;
    SerdStatus status;

    common::ValueVector* sVector; // subject
    common::ValueVector* pVector; // predicate
    common::ValueVector* oVector; // object

    std::unique_ptr<common::ValueVector> sOffsetVector;
    std::unique_ptr<common::ValueVector> pOffsetVector;
    std::unique_ptr<common::ValueVector> oOffsetVector;
};

} // namespace processor
} // namespace kuzu

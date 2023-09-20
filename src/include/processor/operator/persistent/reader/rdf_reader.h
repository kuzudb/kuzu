#pragma once

#include "common/data_chunk/data_chunk.h"
#include "common/string_utils.h"
#include "serd.h"

namespace kuzu {
namespace processor {

class RDFReader {
public:
    explicit RDFReader(std::string filePath);

    ~RDFReader();

    common::offset_t read(common::DataChunk* dataChunkToRead);

private:
    static SerdStatus errorHandle(void* handle, const SerdError* error);
    static SerdStatus handleStatements(void* handle, SerdStatementFlags flags,
        const SerdNode* graph, const SerdNode* subject, const SerdNode* predicate,
        const SerdNode* object, const SerdNode* object_datatype, const SerdNode* object_lang);
    static bool isSerdTypeSupported(SerdType serdType);

private:
    const std::string filePath;
    FILE* fp;
    SerdReader* reader;
    common::offset_t numLinesRead;
    SerdStatus status;
    common::ValueVector* subjectVector;
    common::ValueVector* predicateVector;
    common::ValueVector* objectVector;
};

} // namespace processor
} // namespace kuzu

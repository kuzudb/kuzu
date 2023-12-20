#pragma once

#include "common/data_chunk/data_chunk.h"
#include "function/table_functions.h"
#include "processor/operator/persistent/reader/rdf/rdf_reader_mode.h"
#include "serd.h"

namespace kuzu {
namespace processor {

class RdfReader {
public:
    RdfReader(std::string filePath, common::FileType fileType, RdfReaderMode mode)
        : filePath{std::move(filePath)}, fileType{fileType}, mode{mode}, reader{nullptr},
          rowOffset{0}, vectorSize{0}, status{SERD_SUCCESS}, sVector{nullptr}, pVector{nullptr},
          oVector{nullptr} {}

    ~RdfReader();

    void initReader();

    common::offset_t read(common::DataChunk* dataChunkToRead);

private:
    static SerdStatus errorHandle(void* handle, const SerdError* error);

    static SerdStatus rHandle(void* handle, SerdStatementFlags flags, const SerdNode* graph,
        const SerdNode* subject, const SerdNode* predicate, const SerdNode* object,
        const SerdNode* object_datatype, const SerdNode* object_lang);
    static SerdStatus lHandle(void* handle, SerdStatementFlags flags, const SerdNode* graph,
        const SerdNode* subject, const SerdNode* predicate, const SerdNode* object,
        const SerdNode* object_datatype, const SerdNode* object_lang);
    static SerdStatus rrrHandle(void* handle, SerdStatementFlags flags, const SerdNode* graph,
        const SerdNode* subject, const SerdNode* predicate, const SerdNode* object,
        const SerdNode* object_datatype, const SerdNode* object_lang);
    static SerdStatus rrlHandle(void* handle, SerdStatementFlags flags, const SerdNode* graph,
        const SerdNode* subject, const SerdNode* predicate, const SerdNode* object,
        const SerdNode* object_datatype, const SerdNode* object_lang);

    static SerdStatus prefixHandle(void* handle, const SerdNode* name, const SerdNode* uri);

private:
    const std::string filePath;
    common::FileType fileType;
    RdfReaderMode mode;

    FILE* fp;
    SerdReader* reader;

    // TODO(Xiyang): use prefix to expand CURIE.
    const char* currentPrefix;
    common::offset_t rowOffset;
    common::offset_t vectorSize;
    SerdStatus status;

    common::ValueVector* sVector; // subject
    common::ValueVector* pVector; // predicate
    common::ValueVector* oVector; // object
};

} // namespace processor
} // namespace kuzu

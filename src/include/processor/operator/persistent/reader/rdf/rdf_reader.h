#pragma once

#include "common/copier_config/rdf_reader_config.h"
#include "common/copier_config/reader_config.h"
#include "common/data_chunk/data_chunk.h"
#include "serd.h"
#include "triple_store.h"

namespace kuzu {
namespace processor {

class RdfReader {
public:
    RdfReader(std::string filePath, common::FileType fileType, common::RdfReaderMode mode,
        RdfStore* store)
        : filePath{std::move(filePath)}, fileType{fileType}, mode{mode}, store{store},
          reader{nullptr}, rowOffset{0}, status{SERD_SUCCESS}, dataChunk{nullptr} {}

    ~RdfReader();

    void initReader();

    common::offset_t read(common::DataChunk* dataChunk);

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
    static SerdStatus allHandle(void* handle, SerdStatementFlags flags, const SerdNode* graph,
        const SerdNode* subject, const SerdNode* predicate, const SerdNode* object,
        const SerdNode* object_datatype, const SerdNode* object_lang);

    static SerdStatus prefixHandle(void* handle, const SerdNode* name, const SerdNode* uri);

private:
    const std::string filePath;
    common::FileType fileType;
    common::RdfReaderMode mode;
    RdfStore* store;

    FILE* fp;
    SerdReader* reader;

    // TODO(Xiyang): use prefix to expand CURIE.
    const char* currentPrefix;
    common::offset_t rowOffset;
    SerdStatus status;

    common::DataChunk* dataChunk;
};

} // namespace processor
} // namespace kuzu

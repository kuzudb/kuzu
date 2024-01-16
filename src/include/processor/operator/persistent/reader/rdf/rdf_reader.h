#pragma once

#include "common/copier_config/reader_config.h"
#include "common/data_chunk/data_chunk.h"
#include "serd.h"
#include "triple_store.h"

namespace kuzu {
namespace processor {

class RdfReader {
public:
    RdfReader(std::string filePath, common::FileType fileType, RdfStore* store_)
        : store_{store_}, cursor{0}, rowOffset{0}, filePath{std::move(filePath)},
          fileType{fileType}, reader{nullptr}, status{SERD_SUCCESS} {}

    virtual ~RdfReader();

    virtual void init() = 0;

    common::offset_t readChunk(common::DataChunk* dataChunk);
    void readAll();
    void addNode(std::vector<std::string>& vector, const SerdNode* node);

protected:
    void initInternal(SerdStatementSink statementHandle);

    virtual uint64_t readToVector(common::DataChunk* dataChunk) = 0;

    static SerdStatus errorHandle(void* handle, const SerdError* error);
    static SerdStatus prefixHandle(void* handle, const SerdNode* nameNode, const SerdNode* uriNode);

public:
    RdfStore* store_;

protected:
    uint64_t cursor = 0;
    common::offset_t rowOffset;

private:
    std::string filePath;
    common::FileType fileType;

    FILE* fp;
    SerdReader* reader;

    std::string defaultPrefix;
    std::unordered_map<std::string, std::string> prefixMap;
    SerdStatus status;
};

class RdfResourceReader final : public RdfReader {
public:
    RdfResourceReader(std::string filePath, common::FileType fileType, RdfStore* store_)
        : RdfReader{filePath, fileType, store_} {}

    inline void init() override { initInternal(handle); }

private:
    static SerdStatus handle(void* handle, SerdStatementFlags flags, const SerdNode* graph,
        const SerdNode* subject, const SerdNode* predicate, const SerdNode* object,
        const SerdNode* object_datatype, const SerdNode* object_lang);

    uint64_t readToVector(common::DataChunk* dataChunk) override;
};

class RdfLiteralReader final : public RdfReader {
public:
    RdfLiteralReader(std::string filePath, common::FileType fileType, RdfStore* store_)
        : RdfReader{filePath, fileType, store_} {}

    inline void init() override { initInternal(handle); }

private:
    static SerdStatus handle(void* handle, SerdStatementFlags flags, const SerdNode* graph,
        const SerdNode* subject, const SerdNode* predicate, const SerdNode* object,
        const SerdNode* object_datatype, const SerdNode* object_lang);

    uint64_t readToVector(common::DataChunk* dataChunk) override;
};

class RdfResourceTripleReader final : public RdfReader {
public:
    RdfResourceTripleReader(std::string filePath, common::FileType fileType, RdfStore* store_)
        : RdfReader{filePath, fileType, store_} {}

    inline void init() override { initInternal(handle); }

private:
    static SerdStatus handle(void* handle, SerdStatementFlags flags, const SerdNode* graph,
        const SerdNode* subject, const SerdNode* predicate, const SerdNode* object,
        const SerdNode* object_datatype, const SerdNode* object_lang);

    uint64_t readToVector(common::DataChunk* dataChunk) override;
};

class RdfLiteralTripleReader final : public RdfReader {
public:
    RdfLiteralTripleReader(std::string filePath, common::FileType fileType, RdfStore* store_)
        : RdfReader{filePath, fileType, store_} {}

    inline void init() override { initInternal(handle); }

private:
    static SerdStatus handle(void* handle, SerdStatementFlags flags, const SerdNode* graph,
        const SerdNode* subject, const SerdNode* predicate, const SerdNode* object,
        const SerdNode* object_datatype, const SerdNode* object_lang);

    uint64_t readToVector(common::DataChunk* dataChunk) override;
};

class RdfTripleReader final : public RdfReader {
public:
    RdfTripleReader(std::string filePath, common::FileType fileType, RdfStore* store_)
        : RdfReader{filePath, fileType, store_} {}

    inline void init() override { initInternal(handle); }

private:
    static SerdStatus handle(void* handle, SerdStatementFlags flags, const SerdNode* graph,
        const SerdNode* subject, const SerdNode* predicate, const SerdNode* object,
        const SerdNode* object_datatype, const SerdNode* object_lang);

    uint64_t readToVector(common::DataChunk*) override { KU_UNREACHABLE; }
};

} // namespace processor
} // namespace kuzu

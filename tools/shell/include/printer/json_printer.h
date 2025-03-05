#pragma once

#include "common/data_chunk/data_chunk_state.h"
#include "main/query_result.h"
#include "printer.h"

namespace kuzu {
namespace main {

class JsonPrinter : public Printer {
protected:
    static constexpr const char* NEW_LINE = "\n";

public:
    explicit JsonPrinter(PrinterType type);

    std::string print(QueryResult& queryResult, storage::MemoryManager& mm) const;

    bool defaultPrintStats() const override { return false; }

private:
    virtual std::string printHeader() const = 0;
    virtual std::string printFooter() const = 0;
    virtual std::string printDelim() const = 0;
    std::string printBody(QueryResult& queryResult, storage::MemoryManager& mm) const;

private:
    std::shared_ptr<common::DataChunkState> resultVectorState;
};

class ArrayJsonPrinter : public JsonPrinter {
private:
    static constexpr const char* ARRAY_OPEN = "[\n";
    static constexpr const char* ARRAY_CLOSE = "]\n";

public:
    ArrayJsonPrinter() : JsonPrinter{PrinterType::JSON} {}

private:
    std::string printHeader() const override { return ARRAY_OPEN; }
    std::string printFooter() const override { return ARRAY_CLOSE; }
    std::string printDelim() const override { return std::string(","); };
};

class NDJsonPrinter : public JsonPrinter {
public:
    NDJsonPrinter() : JsonPrinter{PrinterType::JSONLINES} {}

private:
    std::string printHeader() const override { return ""; }
    std::string printFooter() const override { return ""; }
    std::string printDelim() const override { return ""; }
};

} // namespace main
} // namespace kuzu

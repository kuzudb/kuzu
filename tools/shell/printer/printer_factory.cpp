#include "printer/printer_factory.h"

#include "printer/json_printer.h"

namespace kuzu {
namespace main {

std::unique_ptr<Printer> PrinterFactory::getPrinter(PrinterType type) {
    switch (type) {
    case PrinterType::BOX:
        return std::make_unique<BoxPrinter>();
    case PrinterType::TABLE:
        return std::make_unique<TablePrinter>();
    case PrinterType::CSV:
        return std::make_unique<CSVPrinter>();
    case PrinterType::TSV:
        return std::make_unique<TSVPrinter>();
    case PrinterType::MARKDOWN:
        return std::make_unique<MarkdownPrinter>();
    case PrinterType::COLUMN:
        return std::make_unique<ColumnPrinter>();
    case PrinterType::LIST:
        return std::make_unique<ListPrinter>();
    case PrinterType::TRASH:
        return std::make_unique<TrashPrinter>();
    case PrinterType::JSON:
        return std::make_unique<ArrayJsonPrinter>();
    case PrinterType::JSONLINES:
        return std::make_unique<NDJsonPrinter>();
    case PrinterType::HTML:
        return std::make_unique<HTMLPrinter>();
    case PrinterType::LATEX:
        return std::make_unique<LatexPrinter>();
    case PrinterType::LINE:
        return std::make_unique<LinePrinter>();
    default:
        return nullptr;
    }
}

} // namespace main
} // namespace kuzu

#pragma once

#include <algorithm>
#include <cstdint>
#include <string>

#include "common/cast.h"

namespace kuzu {
namespace main {

enum class PrinterType : uint8_t {
    BOX,
    TABLE,
    CSV,
    TSV,
    MARKDOWN,
    COLUMN,
    LIST,
    TRASH,
    JSON,
    JSONLINES,
    HTML,
    LATEX,
    LINE,
    UNKNOWN,
};

struct PrinterTypeUtils {
    static PrinterType fromString(const std::string& str) {
        std::string strLower = str;
        std::transform(strLower.begin(), strLower.end(), strLower.begin(), ::tolower);
        if (str == "box") {
            return PrinterType::BOX;
        } else if (str == "table") {
            return PrinterType::TABLE;
        } else if (str == "csv") {
            return PrinterType::CSV;
        } else if (str == "tsv") {
            return PrinterType::TSV;
        } else if (str == "markdown") {
            return PrinterType::MARKDOWN;
        } else if (str == "column") {
            return PrinterType::COLUMN;
        } else if (str == "list") {
            return PrinterType::LIST;
        } else if (str == "trash") {
            return PrinterType::TRASH;
        } else if (str == "json") {
            return PrinterType::JSON;
        } else if (str == "jsonlines") {
            return PrinterType::JSONLINES;
        } else if (str == "html") {
            return PrinterType::HTML;
        } else if (str == "latex") {
            return PrinterType::LATEX;
        } else if (str == "line") {
            return PrinterType::LINE;
        }
        return PrinterType::UNKNOWN;
    }

    static bool isTableType(PrinterType type) {
        return type == PrinterType::TABLE || type == PrinterType::BOX ||
               type == PrinterType::MARKDOWN || type == PrinterType::COLUMN;
    }
};

struct Printer {
    const PrinterType printType;
    const char* TupleDelimiter = " ";

    template<class TARGET>
    const TARGET& constCast() const {
        return common::ku_dynamic_cast<const TARGET&>(*this);
    }

    virtual ~Printer() = default;

    virtual bool defaultPrintStats() const { return true; }

protected:
    explicit Printer(PrinterType pt) : printType(pt) {}
};

// TODO(Ziyi): Refactor printers to separate files.
struct BaseTablePrinter : public Printer {
    static constexpr uint64_t SMALL_TABLE_SEPERATOR_LENGTH = 3;
    static constexpr uint64_t MIN_TRUNCATED_WIDTH = 20;

    const char* DownAndRight = "";
    const char* Horizontal = "";
    const char* DownAndHorizontal = "";
    const char* DownAndLeft = "";
    const char* Vertical = "";
    const char* VerticalAndRight = "";
    const char* VerticalAndHorizontal = "";
    const char* VerticalAndLeft = "";
    const char* UpAndRight = "";
    const char* UpAndHorizontal = "";
    const char* UpAndLeft = "";
    const char* MiddleDot = ".";
    const char* Truncation = "...";
    bool TopLine = true;
    bool BottomLine = true;
    bool Types = true;

protected:
    explicit BaseTablePrinter(PrinterType pt) : Printer(pt){};
};

struct BoxPrinter : public BaseTablePrinter {
    BoxPrinter() : BaseTablePrinter(PrinterType::BOX) {
        DownAndRight = "\u250C";
        Horizontal = "\u2500";
        DownAndHorizontal = "\u252C";
        DownAndLeft = "\u2510";
        Vertical = "\u2502";
        VerticalAndRight = "\u251C";
        VerticalAndHorizontal = "\u253C";
        VerticalAndLeft = "\u2524";
        UpAndRight = "\u2514";
        UpAndHorizontal = "\u2534";
        UpAndLeft = "\u2518";
        MiddleDot = "\u00B7";
    }
};

struct TablePrinter : public BaseTablePrinter {
    TablePrinter() : BaseTablePrinter(PrinterType::TABLE) {
        DownAndRight = "+";
        Horizontal = "-";
        DownAndHorizontal = "+";
        DownAndLeft = "+";
        Vertical = "|";
        VerticalAndRight = "+";
        VerticalAndHorizontal = "+";
        VerticalAndLeft = "+";
        UpAndRight = "+";
        UpAndHorizontal = "+";
        UpAndLeft = "+";
    }
};

struct CSVPrinter : public Printer {
    CSVPrinter() : Printer(PrinterType::CSV) { TupleDelimiter = ","; }
};

struct TSVPrinter : public Printer {
    TSVPrinter() : Printer(PrinterType::TSV) { TupleDelimiter = "\t"; }
};

struct MarkdownPrinter : public BaseTablePrinter {
    MarkdownPrinter() : BaseTablePrinter(PrinterType::MARKDOWN) {
        DownAndRight = "|";
        Horizontal = "-";
        DownAndHorizontal = "|";
        DownAndLeft = "|";
        Vertical = "|";
        VerticalAndRight = "|";
        VerticalAndHorizontal = "|";
        VerticalAndLeft = "|";
        UpAndRight = "|";
        UpAndHorizontal = "|";
        UpAndLeft = "|";
        TopLine = false;
        BottomLine = false;
        Types = false;
    }
};

struct ColumnPrinter : public BaseTablePrinter {
    ColumnPrinter() : BaseTablePrinter(PrinterType::COLUMN) {
        DownAndRight = "";
        Horizontal = "-";
        DownAndHorizontal = "";
        DownAndLeft = "";
        Vertical = " ";
        VerticalAndRight = " ";
        VerticalAndHorizontal = " ";
        VerticalAndLeft = "";
        UpAndRight = "";
        UpAndHorizontal = "";
        UpAndLeft = "";
        TopLine = false;
        BottomLine = false;
    }
};

struct ListPrinter : public Printer {
    ListPrinter() : Printer(PrinterType::LIST) { TupleDelimiter = "|"; }
};

struct TrashPrinter : public Printer {
    TrashPrinter() : Printer(PrinterType::TRASH) {}
};

struct HTMLPrinter : public Printer {
    const char* TableOpen = "<table>";
    const char* TableClose = "</table>";
    const char* RowOpen = "<tr>";
    const char* RowClose = "</tr>";
    const char* CellOpen = "<td>";
    const char* CellClose = "</td>";
    const char* HeaderOpen = "<th>";
    const char* HeaderClose = "</th>";
    const char* LineBreak = "<br>";
    HTMLPrinter() : Printer(PrinterType::HTML) {}
};

struct LatexPrinter : public Printer {
    const char* TableOpen = "\\begin{tabular}";
    const char* TableClose = "\\end{tabular}";
    const char* AlignOpen = "{";
    const char* AlignClose = "}";
    const char* Line = "\\hline";
    const char* EndLine = "\\\\";
    const char* ColumnAlign = "l";
    LatexPrinter() : Printer(PrinterType::LATEX) { TupleDelimiter = "&"; }
};

struct LinePrinter : public Printer {
    LinePrinter() : Printer(PrinterType::LINE) { TupleDelimiter = " = "; }
};

} // namespace main
} // namespace kuzu

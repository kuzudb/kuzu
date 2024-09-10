#include <algorithm>
#include <cstdint>
#include <string>

namespace kuzu {
namespace main {

enum class PrintType : uint8_t {
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

struct PrintTypeUtils {
    static PrintType fromString(const std::string& str) {
        std::string strLower = str;
        std::transform(strLower.begin(), strLower.end(), strLower.begin(), ::tolower);
        if (str == "box") {
            return PrintType::BOX;
        } else if (str == "table") {
            return PrintType::TABLE;
        } else if (str == "csv") {
            return PrintType::CSV;
        } else if (str == "tsv") {
            return PrintType::TSV;
        } else if (str == "markdown") {
            return PrintType::MARKDOWN;
        } else if (str == "column") {
            return PrintType::COLUMN;
        } else if (str == "list") {
            return PrintType::LIST;
        } else if (str == "trash") {
            return PrintType::TRASH;
        } else if (str == "json") {
            return PrintType::JSON;
        } else if (str == "jsonlines") {
            return PrintType::JSONLINES;
        } else if (str == "html") {
            return PrintType::HTML;
        } else if (str == "latex") {
            return PrintType::LATEX;
        } else if (str == "line") {
            return PrintType::LINE;
        }
        return PrintType::UNKNOWN;
    }

    static bool isTableType(PrintType type) {
        return type == PrintType::TABLE || type == PrintType::BOX || type == PrintType::MARKDOWN ||
               type == PrintType::COLUMN;
    }
};

struct DrawingCharacters {
    const PrintType printType;
    const char* TupleDelimiter = " ";

    virtual ~DrawingCharacters() = default;

protected:
    explicit DrawingCharacters(PrintType pt) : printType(pt) {}
};

struct BaseTableDrawingCharacters : public DrawingCharacters {
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
    explicit BaseTableDrawingCharacters(PrintType pt) : DrawingCharacters(pt){};
};

struct BoxDrawingCharacters : public BaseTableDrawingCharacters {
    BoxDrawingCharacters() : BaseTableDrawingCharacters(PrintType::BOX) {
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

struct TableDrawingCharacters : public BaseTableDrawingCharacters {
    TableDrawingCharacters() : BaseTableDrawingCharacters(PrintType::TABLE) {
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

struct CSVDrawingCharacters : public DrawingCharacters {
    CSVDrawingCharacters() : DrawingCharacters(PrintType::CSV) { TupleDelimiter = ","; }
};

struct TSVDrawingCharacters : public DrawingCharacters {
    TSVDrawingCharacters() : DrawingCharacters(PrintType::TSV) { TupleDelimiter = "\t"; }
};

struct MarkdownDrawingCharacters : public BaseTableDrawingCharacters {
    MarkdownDrawingCharacters() : BaseTableDrawingCharacters(PrintType::MARKDOWN) {
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

struct ColumnDrawingCharacters : public BaseTableDrawingCharacters {
    ColumnDrawingCharacters() : BaseTableDrawingCharacters(PrintType::COLUMN) {
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

struct ListDrawingCharacters : public DrawingCharacters {
    ListDrawingCharacters() : DrawingCharacters(PrintType::LIST) { TupleDelimiter = "|"; }
};

struct TrashDrawingCharacters : public DrawingCharacters {
    TrashDrawingCharacters() : DrawingCharacters(PrintType::TRASH) {}
};

struct JSONDrawingCharacters : public DrawingCharacters {
    const char* ArrayOpen = "[";
    const char* ArrayClose = "]";
    const char* ObjectOpen = "{";
    const char* ObjectClose = "}";
    const char* KeyValue = "\"";
    const char* KeyDelimiter = ":";
    JSONDrawingCharacters() : DrawingCharacters(PrintType::JSON) { TupleDelimiter = ","; }

protected:
    explicit JSONDrawingCharacters(PrintType pt) : DrawingCharacters(pt) {}
};

struct JSONLinesDrawingCharacters : public JSONDrawingCharacters {
    JSONLinesDrawingCharacters() : JSONDrawingCharacters(PrintType::JSONLINES) {
        TupleDelimiter = ",";
    }
};

struct HTMLDrawingCharacters : public DrawingCharacters {
    const char* TableOpen = "<table>";
    const char* TableClose = "</table>";
    const char* RowOpen = "<tr>";
    const char* RowClose = "</tr>";
    const char* CellOpen = "<td>";
    const char* CellClose = "</td>";
    const char* HeaderOpen = "<th>";
    const char* HeaderClose = "</th>";
    const char* LineBreak = "<br>";
    HTMLDrawingCharacters() : DrawingCharacters(PrintType::HTML) {}
};

struct LatexDrawingCharacters : public DrawingCharacters {
    const char* TableOpen = "\\begin{tabular}";
    const char* TableClose = "\\end{tabular}";
    const char* AlignOpen = "{";
    const char* AlignClose = "}";
    const char* Line = "\\hline";
    const char* EndLine = "\\\\";
    const char* ColumnAlign = "l";
    LatexDrawingCharacters() : DrawingCharacters(PrintType::LATEX) { TupleDelimiter = "&"; }
};

struct LineDrawingCharacters : public DrawingCharacters {
    LineDrawingCharacters() : DrawingCharacters(PrintType::LINE) { TupleDelimiter = " = "; }
};

} // namespace main
} // namespace kuzu

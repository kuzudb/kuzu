
// Generated from Cypher.g4 by ANTLR 4.13.1

#pragma once


#include "antlr4-runtime.h"




class  CypherLexer : public antlr4::Lexer {
public:
  enum {
    T__0 = 1, T__1 = 2, T__2 = 3, T__3 = 4, T__4 = 5, T__5 = 6, T__6 = 7, 
    T__7 = 8, T__8 = 9, T__9 = 10, T__10 = 11, T__11 = 12, T__12 = 13, T__13 = 14, 
    T__14 = 15, T__15 = 16, T__16 = 17, T__17 = 18, T__18 = 19, T__19 = 20, 
    T__20 = 21, T__21 = 22, T__22 = 23, T__23 = 24, T__24 = 25, T__25 = 26, 
    T__26 = 27, T__27 = 28, T__28 = 29, T__29 = 30, T__30 = 31, T__31 = 32, 
    T__32 = 33, T__33 = 34, T__34 = 35, T__35 = 36, T__36 = 37, T__37 = 38, 
    T__38 = 39, T__39 = 40, T__40 = 41, T__41 = 42, T__42 = 43, T__43 = 44, 
    T__44 = 45, ATTACH = 46, DBTYPE = 47, USE = 48, CALL = 49, COMMENT_ = 50, 
    MACRO = 51, GLOB = 52, COPY = 53, FROM = 54, COLUMN = 55, EXPORT = 56, 
    IMPORT = 57, DATABASE = 58, NODE = 59, TABLE = 60, GROUP = 61, RDFGRAPH = 62, 
    SEQUENCE = 63, INCREMENT = 64, MINVALUE = 65, MAXVALUE = 66, START = 67, 
    NO = 68, CYCLE = 69, DROP = 70, ALTER = 71, DEFAULT = 72, RENAME = 73, 
    ADD = 74, PRIMARY = 75, KEY = 76, REL = 77, TO = 78, DECIMAL = 79, EXPLAIN = 80, 
    PROFILE = 81, BEGIN = 82, TRANSACTION = 83, READ = 84, ONLY = 85, WRITE = 86, 
    COMMIT = 87, COMMIT_SKIP_CHECKPOINT = 88, ROLLBACK = 89, ROLLBACK_SKIP_CHECKPOINT = 90, 
    INSTALL = 91, EXTENSION = 92, UNION = 93, ALL = 94, LOAD = 95, HEADERS = 96, 
    OPTIONAL = 97, MATCH = 98, UNWIND = 99, CREATE = 100, MERGE = 101, ON = 102, 
    SET = 103, DETACH = 104, DELETE = 105, WITH = 106, RETURN = 107, DISTINCT = 108, 
    STAR = 109, AS = 110, ORDER = 111, BY = 112, L_SKIP = 113, LIMIT = 114, 
    ASCENDING = 115, ASC = 116, DESCENDING = 117, DESC = 118, WHERE = 119, 
    SHORTEST = 120, OR = 121, XOR = 122, AND = 123, NOT = 124, INVALID_NOT_EQUAL = 125, 
    MINUS = 126, FACTORIAL = 127, COLON = 128, IN = 129, STARTS = 130, ENDS = 131, 
    CONTAINS = 132, IS = 133, NULL_ = 134, TRUE = 135, FALSE = 136, COUNT = 137, 
    EXISTS = 138, CASE = 139, ELSE = 140, END = 141, WHEN = 142, THEN = 143, 
    StringLiteral = 144, EscapedChar = 145, DecimalInteger = 146, HexLetter = 147, 
    HexDigit = 148, Digit = 149, NonZeroDigit = 150, NonZeroOctDigit = 151, 
    ZeroDigit = 152, RegularDecimalReal = 153, UnescapedSymbolicName = 154, 
    IdentifierStart = 155, IdentifierPart = 156, EscapedSymbolicName = 157, 
    SP = 158, WHITESPACE = 159, Comment = 160, Unknown = 161
  };

  explicit CypherLexer(antlr4::CharStream *input);

  ~CypherLexer() override;


  std::string getGrammarFileName() const override;

  const std::vector<std::string>& getRuleNames() const override;

  const std::vector<std::string>& getChannelNames() const override;

  const std::vector<std::string>& getModeNames() const override;

  const antlr4::dfa::Vocabulary& getVocabulary() const override;

  antlr4::atn::SerializedATNView getSerializedATN() const override;

  const antlr4::atn::ATN& getATN() const override;

  // By default the static state used to implement the lexer is lazily initialized during the first
  // call to the constructor. You can call this function if you wish to initialize the static state
  // ahead of time.
  static void initialize();

private:

  // Individual action functions triggered by action() above.

  // Individual semantic predicate functions triggered by sempred() above.

};


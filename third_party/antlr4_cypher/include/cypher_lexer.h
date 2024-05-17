
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
    ADD = 74, PRIMARY = 75, KEY = 76, REL = 77, TO = 78, EXPLAIN = 79, PROFILE = 80, 
    BEGIN = 81, TRANSACTION = 82, READ = 83, ONLY = 84, WRITE = 85, COMMIT = 86, 
    COMMIT_SKIP_CHECKPOINT = 87, ROLLBACK = 88, ROLLBACK_SKIP_CHECKPOINT = 89, 
    INSTALL = 90, EXTENSION = 91, UNION = 92, ALL = 93, LOAD = 94, HEADERS = 95, 
    OPTIONAL = 96, MATCH = 97, UNWIND = 98, CREATE = 99, MERGE = 100, ON = 101, 
    SET = 102, DETACH = 103, DELETE = 104, WITH = 105, RETURN = 106, DISTINCT = 107, 
    STAR = 108, AS = 109, ORDER = 110, BY = 111, L_SKIP = 112, LIMIT = 113, 
    ASCENDING = 114, ASC = 115, DESCENDING = 116, DESC = 117, WHERE = 118, 
    SHORTEST = 119, OR = 120, XOR = 121, AND = 122, NOT = 123, INVALID_NOT_EQUAL = 124, 
    MINUS = 125, FACTORIAL = 126, COLON = 127, IN = 128, STARTS = 129, ENDS = 130, 
    CONTAINS = 131, IS = 132, NULL_ = 133, TRUE = 134, FALSE = 135, COUNT = 136, 
    EXISTS = 137, CASE = 138, ELSE = 139, END = 140, WHEN = 141, THEN = 142, 
    StringLiteral = 143, EscapedChar = 144, DecimalInteger = 145, HexLetter = 146, 
    HexDigit = 147, Digit = 148, NonZeroDigit = 149, NonZeroOctDigit = 150, 
    ZeroDigit = 151, RegularDecimalReal = 152, UnescapedSymbolicName = 153, 
    IdentifierStart = 154, IdentifierPart = 155, EscapedSymbolicName = 156, 
    SP = 157, WHITESPACE = 158, Comment = 159, Unknown = 160
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


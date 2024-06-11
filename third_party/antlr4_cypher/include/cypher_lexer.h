
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
    T__44 = 45, ADD = 46, ALL = 47, ALTER = 48, AND = 49, AS = 50, ASC = 51, 
    ASCENDING = 52, ATTACH = 53, BEGIN = 54, BY = 55, CALL = 56, CASE = 57, 
    CAST = 58, COLUMN = 59, COMMENT = 60, COMMIT = 61, COMMIT_SKIP_CHECKPOINT = 62, 
    CONTAINS = 63, COPY = 64, COUNT = 65, CREATE = 66, CYCLE = 67, DATABASE = 68, 
    DBTYPE = 69, DEFAULT = 70, DELETE = 71, DESC = 72, DESCENDING = 73, 
    DETACH = 74, DISTINCT = 75, DROP = 76, ELSE = 77, END = 78, ENDS = 79, 
    EXISTS = 80, EXPLAIN = 81, EXPORT = 82, EXTENSION = 83, FALSE = 84, 
    FROM = 85, GLOB = 86, GRAPH = 87, GROUP = 88, HEADERS = 89, IMPORT = 90, 
    IF = 91, IN = 92, INCREMENT = 93, INSTALL = 94, IS = 95, KEY = 96, LIMIT = 97, 
    LOAD = 98, MACRO = 99, MATCH = 100, MAXVALUE = 101, MERGE = 102, MINVALUE = 103, 
    NO = 104, NODE = 105, NOT = 106, NULL_ = 107, ON = 108, ONLY = 109, 
    OPTIONAL = 110, OR = 111, ORDER = 112, PRIMARY = 113, PROFILE = 114, 
    PROJECT = 115, RDFGRAPH = 116, READ = 117, REL = 118, RENAME = 119, 
    RETURN = 120, ROLLBACK = 121, ROLLBACK_SKIP_CHECKPOINT = 122, SEQUENCE = 123, 
    SET = 124, SHORTEST = 125, START = 126, STARTS = 127, TABLE = 128, THEN = 129, 
    TO = 130, TRANSACTION = 131, TRUE = 132, TYPE = 133, UNION = 134, UNWIND = 135, 
    USE = 136, WHEN = 137, WHERE = 138, WITH = 139, WRITE = 140, XOR = 141, 
    DECIMAL = 142, STAR = 143, L_SKIP = 144, INVALID_NOT_EQUAL = 145, MINUS = 146, 
    FACTORIAL = 147, COLON = 148, StringLiteral = 149, EscapedChar = 150, 
    DecimalInteger = 151, HexLetter = 152, HexDigit = 153, Digit = 154, 
    NonZeroDigit = 155, NonZeroOctDigit = 156, ZeroDigit = 157, RegularDecimalReal = 158, 
    UnescapedSymbolicName = 159, IdentifierStart = 160, IdentifierPart = 161, 
    EscapedSymbolicName = 162, SP = 163, WHITESPACE = 164, CypherComment = 165, 
    Unknown = 166
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


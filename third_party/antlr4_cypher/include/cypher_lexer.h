
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
    DBTYPE = 69, DELETE = 70, DESC = 71, DESCENDING = 72, DETACH = 73, DISTINCT = 74, 
    DROP = 75, ELSE = 76, END = 77, ENDS = 78, EXISTS = 79, EXPLAIN = 80, 
    EXPORT = 81, EXTENSION = 82, FALSE = 83, FROM = 84, GLOB = 85, GRAPH = 86, 
    GROUP = 87, HEADERS = 88, HINT = 89, IMPORT = 90, IF = 91, IN = 92, 
    INCREMENT = 93, INSTALL = 94, IS = 95, JOIN = 96, KEY = 97, LIMIT = 98, 
    LOAD = 99, MACRO = 100, MATCH = 101, MAXVALUE = 102, MERGE = 103, MINVALUE = 104, 
    MULTI_JOIN = 105, NO = 106, NODE = 107, NOT = 108, NULL_ = 109, ON = 110, 
    ONLY = 111, OPTIONAL = 112, OR = 113, ORDER = 114, PRIMARY = 115, PROFILE = 116, 
    PROJECT = 117, RDFGRAPH = 118, READ = 119, REL = 120, RENAME = 121, 
    RETURN = 122, ROLLBACK = 123, ROLLBACK_SKIP_CHECKPOINT = 124, SEQUENCE = 125, 
    SET = 126, SHORTEST = 127, START = 128, STARTS = 129, TABLE = 130, THEN = 131, 
    TO = 132, TRANSACTION = 133, TRUE = 134, TYPE = 135, UNION = 136, UNWIND = 137, 
    USE = 138, WHEN = 139, WHERE = 140, WITH = 141, WRITE = 142, XOR = 143, 
    DECIMAL = 144, STAR = 145, L_SKIP = 146, INVALID_NOT_EQUAL = 147, MINUS = 148, 
    FACTORIAL = 149, COLON = 150, StringLiteral = 151, EscapedChar = 152, 
    DecimalInteger = 153, HexLetter = 154, HexDigit = 155, Digit = 156, 
    NonZeroDigit = 157, NonZeroOctDigit = 158, ZeroDigit = 159, RegularDecimalReal = 160, 
    UnescapedSymbolicName = 161, IdentifierStart = 162, IdentifierPart = 163, 
    EscapedSymbolicName = 164, SP = 165, WHITESPACE = 166, CypherComment = 167, 
    Unknown = 168
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


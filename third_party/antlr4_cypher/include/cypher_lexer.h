
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
    T__44 = 45, ANY = 46, ADD = 47, ALL = 48, ALTER = 49, AND = 50, AS = 51, 
    ASC = 52, ASCENDING = 53, ATTACH = 54, BEGIN = 55, BY = 56, CALL = 57, 
    CASE = 58, CAST = 59, CHECKPOINT = 60, COLUMN = 61, COMMENT = 62, COMMIT = 63, 
    COMMIT_SKIP_CHECKPOINT = 64, CONTAINS = 65, COPY = 66, COUNT = 67, CREATE = 68, 
    CYCLE = 69, DATABASE = 70, DBTYPE = 71, DEFAULT = 72, DELETE = 73, DESC = 74, 
    DESCENDING = 75, DETACH = 76, DISTINCT = 77, DROP = 78, ELSE = 79, END = 80, 
    ENDS = 81, EXISTS = 82, EXPLAIN = 83, EXPORT = 84, EXTENSION = 85, FALSE = 86, 
    FROM = 87, GLOB = 88, GRAPH = 89, GROUP = 90, HEADERS = 91, HINT = 92, 
    IMPORT = 93, IF = 94, IN = 95, INCREMENT = 96, INSTALL = 97, IS = 98, 
    JOIN = 99, KEY = 100, LIMIT = 101, LOAD = 102, LOGICAL = 103, MACRO = 104, 
    MATCH = 105, MAXVALUE = 106, MERGE = 107, MINVALUE = 108, MULTI_JOIN = 109, 
    NO = 110, NODE = 111, NOT = 112, NONE = 113, NULL_ = 114, ON = 115, 
    ONLY = 116, OPTIONAL = 117, OR = 118, ORDER = 119, PRIMARY = 120, PROFILE = 121, 
    PROJECT = 122, RDFGRAPH = 123, READ = 124, REL = 125, RENAME = 126, 
    RETURN = 127, ROLLBACK = 128, ROLLBACK_SKIP_CHECKPOINT = 129, SEQUENCE = 130, 
    SET = 131, SHORTEST = 132, START = 133, STARTS = 134, TABLE = 135, THEN = 136, 
    TO = 137, TRANSACTION = 138, TRUE = 139, TYPE = 140, UNION = 141, UNWIND = 142, 
    USE = 143, WHEN = 144, WHERE = 145, WITH = 146, WRITE = 147, XOR = 148, 
    SINGLE = 149, DECIMAL = 150, STAR = 151, L_SKIP = 152, INVALID_NOT_EQUAL = 153, 
    MINUS = 154, FACTORIAL = 155, COLON = 156, StringLiteral = 157, EscapedChar = 158, 
    DecimalInteger = 159, HexLetter = 160, HexDigit = 161, Digit = 162, 
    NonZeroDigit = 163, NonZeroOctDigit = 164, ZeroDigit = 165, RegularDecimalReal = 166, 
    UnescapedSymbolicName = 167, IdentifierStart = 168, IdentifierPart = 169, 
    EscapedSymbolicName = 170, SP = 171, WHITESPACE = 172, CypherComment = 173, 
    Unknown = 174
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


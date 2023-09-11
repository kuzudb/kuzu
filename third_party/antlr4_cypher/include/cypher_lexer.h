
// Generated from Cypher.g4 by ANTLR 4.9

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
    T__44 = 45, T__45 = 46, T__46 = 47, CALL = 48, MACRO = 49, GLOB = 50, 
    COPY = 51, FROM = 52, COLUMN = 53, NODE = 54, TABLE = 55, GROUP = 56, 
    RDF = 57, GRAPH = 58, DROP = 59, ALTER = 60, DEFAULT = 61, RENAME = 62, 
    ADD = 63, PRIMARY = 64, KEY = 65, REL = 66, TO = 67, EXPLAIN = 68, PROFILE = 69, 
    BEGIN = 70, TRANSACTION = 71, READ = 72, WRITE = 73, COMMIT = 74, COMMIT_SKIP_CHECKPOINT = 75, 
    ROLLBACK = 76, ROLLBACK_SKIP_CHECKPOINT = 77, UNION = 78, ALL = 79, 
    OPTIONAL = 80, MATCH = 81, UNWIND = 82, CREATE = 83, MERGE = 84, ON = 85, 
    SET = 86, DELETE = 87, WITH = 88, RETURN = 89, DISTINCT = 90, STAR = 91, 
    AS = 92, ORDER = 93, BY = 94, L_SKIP = 95, LIMIT = 96, ASCENDING = 97, 
    ASC = 98, DESCENDING = 99, DESC = 100, WHERE = 101, SHORTEST = 102, 
    OR = 103, XOR = 104, AND = 105, NOT = 106, INVALID_NOT_EQUAL = 107, 
    MINUS = 108, FACTORIAL = 109, STARTS = 110, ENDS = 111, CONTAINS = 112, 
    IS = 113, NULL_ = 114, TRUE = 115, FALSE = 116, EXISTS = 117, CASE = 118, 
    ELSE = 119, END = 120, WHEN = 121, THEN = 122, StringLiteral = 123, 
    EscapedChar = 124, DecimalInteger = 125, HexLetter = 126, HexDigit = 127, 
    Digit = 128, NonZeroDigit = 129, NonZeroOctDigit = 130, ZeroDigit = 131, 
    RegularDecimalReal = 132, UnescapedSymbolicName = 133, IdentifierStart = 134, 
    IdentifierPart = 135, EscapedSymbolicName = 136, SP = 137, WHITESPACE = 138, 
    Comment = 139, Unknown = 140
  };

  explicit CypherLexer(antlr4::CharStream *input);
  ~CypherLexer();

  virtual std::string getGrammarFileName() const override;
  virtual const std::vector<std::string>& getRuleNames() const override;

  virtual const std::vector<std::string>& getChannelNames() const override;
  virtual const std::vector<std::string>& getModeNames() const override;
  virtual const std::vector<std::string>& getTokenNames() const override; // deprecated, use vocabulary instead
  virtual antlr4::dfa::Vocabulary& getVocabulary() const override;

  virtual const std::vector<uint16_t> getSerializedATN() const override;
  virtual const antlr4::atn::ATN& getATN() const override;

private:
  static std::vector<antlr4::dfa::DFA> _decisionToDFA;
  static antlr4::atn::PredictionContextCache _sharedContextCache;
  static std::vector<std::string> _ruleNames;
  static std::vector<std::string> _tokenNames;
  static std::vector<std::string> _channelNames;
  static std::vector<std::string> _modeNames;

  static std::vector<std::string> _literalNames;
  static std::vector<std::string> _symbolicNames;
  static antlr4::dfa::Vocabulary _vocabulary;
  static antlr4::atn::ATN _atn;
  static std::vector<uint16_t> _serializedATN;


  // Individual action functions triggered by action() above.

  // Individual semantic predicate functions triggered by sempred() above.

  struct Initializer {
    Initializer();
  };
  static Initializer _init;
};


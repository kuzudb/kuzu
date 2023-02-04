
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
    T__44 = 45, T__45 = 46, COPY = 47, FROM = 48, NODE = 49, TABLE = 50, 
    DROP = 51, ALTER = 52, DEFAULT = 53, RENAME = 54, ADD = 55, PRIMARY = 56, 
    KEY = 57, REL = 58, TO = 59, EXPLAIN = 60, PROFILE = 61, UNION = 62, 
    ALL = 63, OPTIONAL = 64, MATCH = 65, UNWIND = 66, CREATE = 67, SET = 68, 
    DELETE = 69, WITH = 70, RETURN = 71, DISTINCT = 72, STAR = 73, AS = 74, 
    ORDER = 75, BY = 76, L_SKIP = 77, LIMIT = 78, ASCENDING = 79, ASC = 80, 
    DESCENDING = 81, DESC = 82, WHERE = 83, OR = 84, XOR = 85, AND = 86, 
    NOT = 87, INVALID_NOT_EQUAL = 88, MINUS = 89, FACTORIAL = 90, STARTS = 91, 
    ENDS = 92, CONTAINS = 93, IS = 94, NULL_ = 95, TRUE = 96, FALSE = 97, 
    EXISTS = 98, CASE = 99, ELSE = 100, END = 101, WHEN = 102, THEN = 103, 
    StringLiteral = 104, EscapedChar = 105, DecimalInteger = 106, HexLetter = 107, 
    HexDigit = 108, Digit = 109, NonZeroDigit = 110, NonZeroOctDigit = 111, 
    ZeroDigit = 112, RegularDecimalReal = 113, UnescapedSymbolicName = 114, 
    IdentifierStart = 115, IdentifierPart = 116, EscapedSymbolicName = 117, 
    SP = 118, WHITESPACE = 119, Comment = 120, Unknown = 121
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


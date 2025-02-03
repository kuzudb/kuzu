#!/usr/bin/env nix-shell

{
  pkgs ? import <nixpkgs> { },
}:

pkgs.mkShell {
  nativeBuildInputs = with pkgs; [
    nodejs
    cmake
    gcc
    emscripten
    pkg-config
  ];

  buildInputs = with pkgs; [
    pkgsStatic.openssl
    duckdb
  ];
}

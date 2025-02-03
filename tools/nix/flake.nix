{
  description = "Kuzu development shell";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem
      (system:
          let pkgs = nixpkgs.legacyPackages.${system}; in
          {
            devShells.default = import ./shell.nix { inherit pkgs; };
            packages.default = pkgs.callPackage ./package.nix { };
          }
      );
}

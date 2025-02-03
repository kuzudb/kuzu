{ lib
, stdenv
, fetchFromGitHub
, cmake
, openssl
, python3
}:

stdenv.mkDerivation (finalAttrs: {
  pname = "kuzu";
  version = "0.7.1";

  src = fetchFromGitHub {
    owner = "kuzudb";
    repo = "kuzu";
    tag = "v${finalAttrs.version}";
    hash = "sha256-Q7MBy4azXsEpuy8G+KrD+jM/amIr9LsS86VXrtL/ODI=";
  };

  outputs = [ "out" "lib" "dev" ];

  nativeBuildInputs = [ cmake python3 ];
  buildInputs = [ openssl ];

  doInstallCheck = true;

  meta = with lib; {
    changelog = "https://github.com/kuzudb/kuzu/releases/tag/v${finalAttrs.version}";
    description = "Embeddable property graph database management system";
    homepage = "https://kuzudb.com/";
    license = licenses.mit;
    mainProgram = "kuzu";
    maintainers = with maintainers; [ sdht0 ];
    platforms = platforms.all;
  };
})

{
  autoPatchelfHook,
  fetchurl,
  lib,
  npmBuildPatch,
  patchelf,
  python3,
  stdenv,
  stdenvNoCC,
  versionCheckHook,
}:
let
  version = "24.22.1";
  releases = {
    aarch64-darwin = {
      platform = "darwin-arm64";
      sha256 = "654ba79de6155647e22321932b45f2a1ccb705976cc337c9b8e278beb085a628";
    };
    x86_64-darwin = {
      platform = "darwin-x64";
      sha256 = "65e450f90a6eaacd5000f611861c4ce65fadc87f813795dc4ed0c0e5f2fca2d4";
    };
    aarch64-linux = {
      platform = "linux-arm64";
      sha256 = "dbabf7a1cbf947e6059ffad2c27b80041db97dea1e553b62afdee7f13fca336e";
    };
    x86_64-linux = {
      platform = "linux-x64";
      sha256 = "6c172a9ab6718f7b801fb5a7bd486af2ee5e32758086a04b84fcce2aa77095e6";
    };
  };
  release = releases.${stdenvNoCC.hostPlatform.system};
  # Nixpkgs' default patchelf (0.19.2 on current rolling) corrupts this prebuilt
  # `node`: growing its program-header table moves .interp, the notes, .gnu.hash,
  # .dynsym, .dynstr and .dynamic into a new writable first PT_LOAD that overlaps
  # the original read-only one. The loader maps the later segment over the overlap,
  # .dynamic becomes read-only, and node dies with SIGSEGV before printing anything
  # (NixOS/patchelf#531, NixOS/nixpkgs#579143; the overlap fix NixOS/patchelf#672
  # is not in a released patchelf yet). patchelf 0.15.2, Nixpkgs' default until
  # 2026, keeps node's layout intact. Listed before autoPatchelfHook, it precedes
  # the stdenv's patchelf on PATH for auto-patchelf and for the fixup phase's
  # RPATH shrinking alike.
  patchelf_0_15 = patchelf.overrideAttrs (
    finalAttrs: _previousAttrs: {
      version = "0.15.2";
      src = fetchurl {
        url = "https://github.com/NixOS/patchelf/releases/download/${finalAttrs.version}/patchelf-${finalAttrs.version}.tar.bz2";
        hash = "sha256-F3RfVkFZyOIo/EEtplogSLhGxLa0Igt3y/IkFuAvLXw=";
      };
    }
  );
in
stdenvNoCC.mkDerivation {
  pname = "nodejs";
  inherit version;
  src = fetchurl {
    url = "https://nodejs.org/dist/v${version}/node-v${version}-${release.platform}.tar.xz";
    inherit (release) sha256;
  };

  outputs = [
    "out"
    "dev"
  ];
  strictDeps = true;
  dontConfigure = true;
  dontBuild = true;
  dontStrip = true;
  # Keep Nixpkgs' offline npm behavior when using the official runtime archive.
  postPatch = ''
    patch -d lib/node_modules/npm -p3 < ${npmBuildPatch}
  '';
  nativeBuildInputs = lib.optionals stdenvNoCC.hostPlatform.isLinux [
    patchelf_0_15
    autoPatchelfHook
  ];
  buildInputs = lib.optionals stdenvNoCC.hostPlatform.isLinux [ stdenv.cc.cc.lib ];
  installPhase = ''
    runHook preInstall
    mkdir -p "$out" "$dev"
    cp -R bin include lib share "$out/"
    for entry in npm-cli.js npx-cli.js; do
      substituteInPlace "$out/lib/node_modules/npm/bin/$entry" \
        --replace-fail '#!/usr/bin/env node' "#!$out/bin/node"
    done
    runHook postInstall
  '';
  doInstallCheck = true;
  nativeInstallCheckInputs = [ versionCheckHook ];
  versionCheckProgramArg = "--version";
  postInstallCheck = ''
    test "$("$out/bin/node" --version)" = "v${version}"
    "$out/bin/npm" --version
  '';
  passthru = {
    python = python3;
    interpreterName = "node";
  };
  meta = {
    description = "Node.js official security release with its bundled TLS verifier";
    homepage = "https://nodejs.org/";
    license = lib.licenses.mit;
    sourceProvenance = [ lib.sourceTypes.binaryNativeCode ];
    platforms = builtins.attrNames releases;
    mainProgram = "node";
  };
}

{
  lib,
  pkgs,
  ...
}:
let
  certificateDirectory = "${pkgs.umCertificateBundle.hashed}/etc/ssl/certs";
  certificateFile = "${pkgs.umCertificateBundle}/etc/ssl/certs/ca-bundle.crt";
in
{
  # Devenv 2.3.1 filters SSL_CERT_FILE from the generic Nix environment.
  # Give every task an explicit default while allowing local test CA overrides.
  options.tasks = lib.mkOption {
    type = lib.types.attrsOf (
      lib.types.submodule {
        config.env.SSL_CERT_FILE = lib.mkDefault certificateFile;
      }
    );
  };

  config = {
    # Keep this module inside the exported CLI tree so standalone validation
    # receives the same certificate policy without importing private files.
    overlays = [
      (final: prev: {
        umCertificateBundle = final.callPackage ./nix/certificate-bundle.nix { };
        nodejs_24 = final.callPackage ./nix/nodejs.nix {
          npmBuildPatch = builtins.head (
            builtins.filter (
              patch: lib.hasSuffix "node-npm-build-npm-package-logic.patch" (toString patch)
            ) prev.nodejs-slim_24.patches
          );
        };
        nodejs-slim_24 = final.nodejs_24;
      })
    ];

    packages = [ pkgs.umCertificateBundle ];

    env.SSL_CERT_FILE = certificateFile;
    env.SSL_CERT_DIR = certificateDirectory;
    env.NIX_SSL_CERT_FILE = certificateFile;
    env.CURL_CA_BUNDLE = certificateFile;
    env.GIT_SSL_CAINFO = certificateFile;
    env.REQUESTS_CA_BUNDLE = certificateFile;
    env.PIP_CERT = certificateFile;

    # Apply the selected bundle after package setup for shell commands.
    enterShell = lib.mkAfter ''
      export SSL_CERT_FILE=${lib.escapeShellArg certificateFile}
    '';
  };
}

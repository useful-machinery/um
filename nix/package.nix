{
  # Defaults to the hash part of the source fileset's store path, so the
  # identity changes with the CLI source and with nothing else.
  buildIdentity ? null,
  cacert,
  craneLib,
  git,
  jq,
  lib,
  python3,
  version,
}:

let
  src = lib.fileset.toSource {
    root = ../.;
    fileset = lib.fileset.unions [
      ../.config/nextest.toml
      ../Cargo.lock
      ../Cargo.toml
      ../crates
      ../docs/workflow-v1.md
      ../examples
      ../schemas
      ../src
      ../tests
      ../vendor
    ];
  };

  resolvedBuildIdentity =
    if buildIdentity != null then
      buildIdentity
    else
      builtins.head (
        lib.splitString "-" (baseNameOf (builtins.unsafeDiscardStringContext (toString src)))
      );

  commonArgs = {
    pname = "useful";
    inherit src;
    strictDeps = true;
  };

  # The dependency-only build is keyed on crate manifests and vendored sources. The
  # build identity changes with every CLI source change and must not reach this
  # derivation, or the cached dependency artifacts would be invalidated by
  # every source change.
  dependencyArgs = commonArgs // {
    version = "0.0.0-deps";
    # Keep the workspace's registry patch real when Crane stubs local crates.
    extraDummyScript = ''
      rm -rf "$out/vendor/rustls-platform-verifier"
      cp -R ${../vendor/rustls-platform-verifier} "$out/vendor/rustls-platform-verifier"
    '';
  };

  cargoArtifacts = craneLib.buildDepsOnly dependencyArgs;

  testProfileEnv = {
    CARGO_PROFILE_DEV_DEBUG = "0";
    CARGO_PROFILE_TEST_DEBUG = "0";
  };

  # Test and release dependency artifacts use different compiler profiles.
  # This input contains manifests and the patched vendor source. A workspace
  # source change always rebuilds the real test binaries and runs nextest.
  testCargoArtifacts = craneLib.buildDepsOnly (
    dependencyArgs
    // {
      pname = "useful-test";
      CARGO_PROFILE = "";
      env = testProfileEnv;
      buildPhaseCargoCommand = "cargo build --locked --workspace --all-targets --all-features";
      doCheck = false;
    }
  );

  certificateEnv = {
    SSL_CERT_FILE = "${cacert}/etc/ssl/certs/ca-bundle.crt";
  };

  # The test gate injects fixed values shaped like a rolling version and build
  # identity, so its result is independent of the package's identity. The tests
  # compare the binary with whichever values were injected, and the package's
  # postInstall verifies the real ones. `checks.useful-cli` in the flake requires
  # this derivation; the package itself does not.
  testVersion = "0.0.0-dev.0+rev.0000000000000000000000000000000000000000";
  testBuildIdentity = "0000000000000000000000000000000000000000";

  testArgs = commonArgs // {
    version = testVersion;
    nativeBuildInputs = [
      git
      python3
    ];
    env = certificateEnv // {
      USEFUL_BUILD_IDENTITY = testBuildIdentity;
      USEFUL_VERSION = testVersion;
    };
  };

  nextestTests = craneLib.cargoNextest (
    testArgs
    // {
      cargoExtraArgs = "--locked --workspace --all-targets --all-features";
      # Crane defaults CARGO_PROFILE to release. The empty value passes no
      # --cargo-profile, so nextest builds Cargo's test profile, unoptimized
      # with debug assertions and overflow checks, as check-suite does
      # (INV-TESTING-23).
      CARGO_PROFILE = "";
      cargoArtifacts = testCargoArtifacts;
      doInstallCargoArtifacts = false;
      env = testArgs.env // testProfileEnv;
      # Tests that spawn internal workers need the ordinary example executable.
      preBuild = ''
        cargo build --locked --workspace --all-targets --all-features
        export USEFUL_TEST_INTERNAL_WORKER_EXECUTABLE="$(realpath "''${CARGO_TARGET_DIR:-target}/debug/examples/internal-worker")"
      '';
    }
  );
in
craneLib.buildPackage (
  commonArgs
  // {
    inherit cargoArtifacts version;
    doCheck = false;

    nativeBuildInputs = [ jq ];

    env = certificateEnv // {
      USEFUL_BUILD_IDENTITY = resolvedBuildIdentity;
      USEFUL_VERSION = version;
    };

    passthru = {
      inherit cargoArtifacts nextestTests testCargoArtifacts;
    };

    postInstall = ''
      expected="useful ${version}"
      for invocation in "version" "--version"; do
        actual="$($out/bin/useful "$invocation")"
        if [ "$actual" != "$expected" ]; then
          echo "unexpected version output for $invocation: $actual" >&2
          echo "expected: $expected" >&2
          exit 1
        fi
      done

      json="$($out/bin/useful version --json)"
      if ! printf '%s\n' "$json" | jq --exit-status \
        --arg buildIdentity ${lib.escapeShellArg resolvedBuildIdentity} \
        --arg executablePath "$out/bin/useful" \
        --arg version ${lib.escapeShellArg version} \
        '. == {
          "schemaVersion": 1,
          "command": "useful",
          "version": $version,
          "executablePath": $executablePath,
          "buildIdentity": $buildIdentity
        }' >/dev/null; then
        echo "unexpected JSON version output: $json" >&2
        exit 1
      fi
    '';

    meta = {
      description = "Command-line interface and runner for Useful Machinery";
      license = lib.licenses.asl20;
      mainProgram = "useful";
      platforms = lib.platforms.unix;
    };
  }
)

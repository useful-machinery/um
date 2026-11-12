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
    pname = "um";
    inherit src;
    strictDeps = true;
  };

  # The dependency-only build is keyed on crate manifests and vendored sources. The
  # build identity changes with every CLI source change and must not reach this
  # derivation, or the cached dependency artifacts would be invalidated by
  # every source change.
  cargoArtifacts = craneLib.buildDepsOnly (
    commonArgs
    // {
      version = "0.0.0-deps";
      # Registry dependencies compile against this local patch, so its API must
      # remain available while Crane replaces the application crates with stubs.
      extraDummyScript = ''
        rm -rf "$out/vendor/rustls-platform-verifier"
        cp -R ${../vendor/rustls-platform-verifier} "$out/vendor/rustls-platform-verifier"
      '';
    }
  );

  certificateEnv = {
    SSL_CERT_FILE = "${cacert}/etc/ssl/certs/ca-bundle.crt";
  };

  # The test gate injects fixed values shaped like a rolling version and build
  # identity, so its result is independent of the package's identity. The tests
  # compare the binary with whichever values were injected, and the package's
  # postInstall verifies the real ones. `checks.um-cli` in the flake requires
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
      UM_BUILD_IDENTITY = testBuildIdentity;
      UM_VERSION = testVersion;
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
      # The package's dependency archive is a release build, which this
      # build cannot use, so it is not taken.
      cargoArtifacts = null;
      doInstallCargoArtifacts = false;
      # preBuild's dev build and the test build share target/debug and its
      # dependency artifacts because their debuginfo settings are equal.
      # Debuginfo is off in both to keep the build directory small, which full
      # debug symbols can exhaust on the validation host before the suite runs.
      env = testArgs.env // {
        CARGO_PROFILE_DEV_DEBUG = "0";
        CARGO_PROFILE_TEST_DEBUG = "0";
      };
      # --all-targets builds the example as a test harness. Tests that spawn
      # internal workers need the ordinary executable supplied explicitly.
      preBuild = ''
        cargo build --locked --workspace --all-features --bins --example internal-worker
        export UM_TEST_INTERNAL_WORKER_EXECUTABLE="$(realpath "''${CARGO_TARGET_DIR:-target}/debug/examples/internal-worker")"
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
      UM_BUILD_IDENTITY = resolvedBuildIdentity;
      UM_VERSION = version;
    };

    passthru = {
      inherit cargoArtifacts nextestTests;
    };

    postInstall = ''
      expected="um ${version}"
      for invocation in "version" "--version"; do
        actual="$($out/bin/um "$invocation")"
        if [ "$actual" != "$expected" ]; then
          echo "unexpected version output for $invocation: $actual" >&2
          echo "expected: $expected" >&2
          exit 1
        fi
      done

      json="$($out/bin/um version --json)"
      if ! printf '%s\n' "$json" | jq --exit-status \
        --arg buildIdentity ${lib.escapeShellArg resolvedBuildIdentity} \
        --arg executablePath "$out/bin/um" \
        --arg version ${lib.escapeShellArg version} \
        '. == {
          "schemaVersion": 1,
          "command": "um",
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
      mainProgram = "um";
      platforms = lib.platforms.unix;
    };
  }
)

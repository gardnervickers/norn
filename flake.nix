{ inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    utils.url = "github:numtide/flake-utils";
    fenix.url = "github:nix-community/fenix";
  };

  outputs =
    {
      self,
      nixpkgs,
      fenix,
      utils,
      ...
    }:
    utils.lib.eachDefaultSystem (
      system:
      let
        overlays = [ fenix.overlays.default ];
        pkgs = import nixpkgs {
          system = system;
          overlays = overlays;
        };
        fx = fenix.packages.${system};
        rust-toolchain-nightly = fx.combine [
          fx.latest.cargo
          fx.latest.rustc
          fx.latest.rust-analyzer
          fx.latest.clippy
          fx.latest.rustfmt
          fx.latest.rust-src
          fx.latest.miri
        ];
        mkLimaApp =
          command:
          {
            type = "app";
            program = toString (
              pkgs.writeShellScript "norn-${command}" ''
                set -euo pipefail
                repo_root="$(${pkgs.git}/bin/git -C "$PWD" rev-parse --show-toplevel 2>/dev/null || pwd)"
                script="$repo_root/hack/lima-norn-uring.sh"

                if [ ! -x "$script" ]; then
                  echo "missing script: $script" >&2
                  exit 1
                fi

                exec "$script" ${command}
              ''
            );
          };
      in
      {
        # Keep `nix build` working for legacy workflows / flake-compat.
        packages.default = self.devShells.${system}.default;

        apps = {
          uring-vm-up = mkLimaApp "up";
          uring-shell = mkLimaApp "shell";
          uring-test = mkLimaApp "test";
        };

        devShells.default = pkgs.mkShell {
          nativeBuildInputs = [
            pkgs.cargo-udeps
            pkgs.cargo-outdated
            rust-toolchain-nightly
          ];
        };
      }
    );
}

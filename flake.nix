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
      in
      {
        # Keep `nix build` working for legacy workflows / flake-compat.
        packages.default = self.devShells.${system}.default;

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

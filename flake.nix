{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    pgmanager.url = "github:onelittle/pgmanager";
  };

  outputs = {
    nixpkgs,
    flake-utils,
    pgmanager,
    ...
  }:
    flake-utils.lib.eachDefaultSystem (
      system: let
        pkgs = nixpkgs.legacyPackages.${system};
      in {
        devShell = pkgs.mkShell {
          buildInputs = with pkgs; [
            cargo
            rustc
          ];

          packages = [
            pgmanager.packages.${system}.pgmanager
          ];
        };
      }
    );
}

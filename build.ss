#!/usr/bin/env gxi

(import :std/build-script)
(defbuild-script
  `((gsc: "db/_duckdb"
          "-cc-options" "-I/opt/homebrew/include"
          "-ld-options" "-L/opt/homebrew/lib -lduckdb")
    (ssi: "db/_duckdb")
    "db/duckdb"))

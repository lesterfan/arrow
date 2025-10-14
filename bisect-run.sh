#!/usr/bin/env bash
set -euo pipefail

# Always start at repo root so git ops work
cd "$(git rev-parse --show-toplevel)"

# Clean any previous overlay, then re-apply the saved test edits
git reset -q --hard
if ! git stash apply --index -q "$(cat .git/BIS_STASH)"; then
  # Your edits don't apply to this commit; skip it
  git reset -q --hard
  exit 125
fi

cd cpp/
rm -rf build
mkdir build
cd build/

# cmake
if ! cmake -DCMAKE_INSTALL_PREFIX=$ARROW_HOME -DCMAKE_INSTALL_LIBDIR=lib -DCMAKE_BUILD_TYPE=Release -DARROW_BUILD_STATIC=OFF -DARROW_DEPENDENCY_SOURCE=SYSTEM -Dxsimd_SOURCE=BUNDLED -DARROW_WITH_BZ2=ON -DARROW_WITH_ZLIB=ON -DARROW_WITH_ZSTD=ON -DARROW_WITH_LZ4=ON -DARROW_WITH_SNAPPY=ON -DARROW_PARQUET=ON -DARROW_FLIGHT=OFF -DPARQUET_REQUIRE_ENCRYPTION=ON -DARROW_PYTHON=ON -DCMAKE_UNITY_BUILD=ON -DARROW_BUILD_TESTS=ON -DGTest_SOURCE=BUNDLED ..; then 
  echo "[bisect] cmake failed"
  exit 125   # skip unbuildable commits
fi

# build
if ! make -j8; then
  echo "[bisect] build failed"
  exit 125   # skip unbuildable commits
fi

# run your test
if ./release/arrow-misc-test --gtest_filter="*Lester*"; then
  echo "[bisect] test PASSED"
  exit 1     # "bad" -> first passing commit
else
  echo "[bisect] test FAILED"
  exit 0     # "good" -> still failing
fi
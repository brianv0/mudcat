#!/usr/bin/env bash
# The mutation gate: seed each known defect, one at a time, and require that the smoke evals or
# the tests detect it. Builds with the cargo feature `mutants`, in its own target directory.
#
# Usage: scripts/mutation-gate.sh [defect ...]     (no argument: the baseline and all defects)
# Result: evals/results/mutation-gate.txt (a run of a subset writes mutation-gate.partial.txt);
#         raw logs in target/mutation-gate/. Exits 1 if a defect is not detected or the baseline fails.
# Needs a PostgreSQL server (scripts/dev-db.sh start). MUDCAT_SERVER is its URL (any database
# name), default postgres://mudcat:mudcat@localhost:54329/postgres. The role needs createdb.
set -uo pipefail
cd "$(dirname "$0")/.."

# The stamp is taken now, before the script replaces its own result file (which is in the
# repository): the commit, and whether tracked files other than the results had changes.
STAMP="$(git rev-parse HEAD) dirty=$([ -n "$(git status --porcelain --untracked-files=no -- ':(top)' ':(top,exclude)evals/results')" ] && echo yes || echo no)"

ALL=(no-invalidation expired-grant-ok no-compartment search-no-recheck no-stream-filter wide-prefix merkle-constant tag-ref-movable no-branch-lock relay-xid-rule)
# the tenant move with more than one server process (tests/tenant_move_processes.rs, T5.4)
ALL+=(no-home-check no-move-fence cleanup-no-drain)
RUNS_TIMING=3   # defects that depend on timing run this many times
TIMING=(relay-xid-rule)

SERVER=${MUDCAT_SERVER:-postgres://mudcat:mudcat@localhost:54329/postgres}
export CARGO_TARGET_DIR=target/mutants
LOG=target/mutation-gate
mkdir -p "$LOG" evals/results
unset MUDCAT_DB MUDCAT_TEST_DB MUDCAT_MUTANT

if [ $# -gt 0 ]; then DEFECTS=("$@"); OUT=evals/results/mutation-gate.partial.txt; else DEFECTS=("${ALL[@]}"); OUT=evals/results/mutation-gate.txt; fi
for d in "${DEFECTS[@]}"; do
  case " ${ALL[*]} " in *" $d "*) ;; *) echo "unknown defect: $d" >&2; exit 2 ;; esac
done

dburl() { echo "${SERVER%/*}/$1"; }
dropdb() {
  command -v psql >/dev/null || return 0
  local i
  for i in 1 2 3; do
    psql "$(dburl postgres)" -qc "drop database if exists \"$1\" with (force)" >>"$LOG/drop.log" 2>&1 && return 0
    sleep 2
  done
  echo "could not drop the database $1 (see $LOG/drop.log)" >&2
  return 0
}
# The dedicated databases that the tests of tenant moves make, and leave when they fail. The
# gate must be the only user of the tests on this server while it runs.
drop_left_databases() {
  command -v psql >/dev/null || return 0
  local d
  for d in $(psql "$(dburl postgres)" -Atc "select datname from pg_database where datname ~ '^mudcat_(ded_mv|uc505f?)_'" 2>>"$LOG/drop.log"); do dropdb "$d"; done
}

echo "building with the feature mutants (target dir $CARGO_TARGET_DIR) ..."
cargo build --features mutants --bins >"$LOG/build.log" 2>&1 || { tail -30 "$LOG/build.log"; exit 2; }
cargo test --features mutants --no-run >>"$LOG/build.log" 2>&1 || { tail -30 "$LOG/build.log"; exit 2; }
[ -f evals/generated/variants.jsonl ] || python3 evals/generate.py --write >/dev/null

# run_one NAME RUN: runs the smoke evals and the tests with the defect NAME ("-" for none)
run_one() {
  local name=$1 run=$2 tag="$1.$2" mut=$1
  [ "$name" = "-" ] && mut=""
  local db="mudcat_gate_$$_$(date +%s)_$RANDOM"
  echo "== $name (run $run): smoke evals" >&2
  MUDCAT_MUTANT=$mut "$CARGO_TARGET_DIR/debug/mudcat-evals" --scale smoke --jobs 6 --server "$SERVER" --out "$LOG/results" >"$LOG/$tag.evals.log" 2>&1
  echo "== $name (run $run): cargo test" >&2
  MUDCAT_MUTANT=$mut MUDCAT_TEST_DB="$(dburl "$db")" cargo test --features mutants --no-fail-fast >"$LOG/$tag.tests.log" 2>&1
  dropdb "$db"
  drop_left_databases
}

# the failed templates and tests of one run, as text
failed_templates() { grep -E '^FAIL ' "$1" | sed -E 's/^FAIL ([A-Z0-9.]+)#.*/\1/' | sort | uniq -c | awk '{printf "%s(%s) ", $2, $1}'; }
failed_tests() { grep -E '^test .* \.\.\. FAILED$' "$1" | sed -E 's/^test (.*) \.\.\. FAILED$/\1/' | sort | tr '\n' ' '; }
ran_ok() { grep -qE 'variants passed' "$1" && grep -qE '^test result' "$2"; }

ROWS=$(mktemp)
status=0
for name in - "${DEFECTS[@]}"; do
  runs=1
  for t in "${TIMING[@]}"; do [ "$name" = "$t" ] && runs=$RUNS_TIMING; done
  detected=0; tpl=""; tst=""; broken=""
  for r in $(seq 1 $runs); do
    run_one "$name" "$r"
    ev="$LOG/$name.$r.evals.log"; ts="$LOG/$name.$r.tests.log"
    ran_ok "$ev" "$ts" || broken="a run did not finish (see $LOG/$name.$r.*.log)"
    a=$(failed_templates "$ev"); b=$(failed_tests "$ts")
    tpl="$tpl$a"; tst="$tst$b"
    if [ -n "$a$b" ]; then detected=$((detected+1)); fi
  done
  if [ "$name" = "-" ]; then
    if [ $detected -eq 0 ] && [ -z "$broken" ]; then verdict="ok (all green)"; else verdict="BASELINE FAILS ${broken}"; status=1; fi
    printf '%s\t%s\t%s\t%s\n' "(no defect)" "${tpl:-none}" "${tst:-none}" "$verdict" >>"$ROWS"
  else
    if [ -n "$broken" ]; then verdict="NOT DETECTED ($broken)"; status=1
    elif [ $detected -eq $runs ]; then verdict="detected"; [ $runs -gt 1 ] && verdict="detected ($detected/$runs runs)"
    else verdict="NOT DETECTED ($detected/$runs runs)"; status=1; fi
    # collapse the repeats of a timing defect
    tpl=$(echo "$tpl" | tr ' ' '\n' | grep . | sort -u | tr '\n' ' ')
    tst=$(echo "$tst" | tr ' ' '\n' | grep . | sort -u | tr '\n' ' ')
    printf '%s\t%s\t%s\t%s\n' "$name" "${tpl:-none}" "${tst:-none}" "$verdict" >>"$ROWS"
  fi
done

{
  echo "mutation gate: $STAMP smoke scale $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "templates: failed variants in parentheses; tests: failed test names"
  echo
  python3 - "$ROWS" <<'PY'
import sys, textwrap
rows=[l.rstrip("\n").split("\t") for l in open(sys.argv[1])]
rows=[["defect","templates that failed","tests that failed","result"]]+rows
w=[max(len(r[i]) for r in rows) for i in range(3)]
w[1]=min(w[1],60); w[2]=min(w[2],60)
def wrap(s,n): return textwrap.wrap(s,n) or [""]
for k,r in enumerate(rows):
    cols=[wrap(r[i],w[i]) for i in range(3)]
    h=max(len(c) for c in cols)
    for j in range(h):
        cells=[(c[j] if j<len(c) else "").ljust(w[i]) for i,c in enumerate(cols)]
        print(" | ".join(cells)+(" | "+r[3] if j==0 else ""))
    if k==0: print("-+-".join("-"*x for x in w)+"-+-"+"-"*8)
PY
} >"$OUT"
rm -f "$ROWS"
cat "$OUT"
exit $status

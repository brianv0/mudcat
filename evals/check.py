#!/usr/bin/env python3
"""Validate the eval scenario files.

Checks the structure of each eval, the action names, the variant names and
the requirement IDs. Prints a summary and the list of gaps. Exits with 1 if
a file is not valid.

Run from the repository root: python3 evals/check.py
"""
import collections
import pathlib
import re
import sys

import yaml

ROOT = pathlib.Path(__file__).resolve().parent.parent
EVALS = ROOT / "evals"

VARIANTS = {
    "happy-path", "concurrency", "policy", "removal",
    "time", "placement", "isolation", "failure",
}
ACTIONS = {
    # namespace and storage
    "create_collection", "register", "verify", "report_inventory", "add_rule", "retire_replica",
    # metadata
    "declare_field", "write_metadata", "tag", "enrich", "review",
    # datasets
    "commit", "publish", "create_ref", "clone", "select", "changes", "export", "mint_doi",
    # policy
    "set_terms", "declare_equivalence", "invalidate", "purge", "erase", "place_hold", "release_hold",
    # access
    "grant", "revoke", "sync_membership", "sign_in", "create_tenant", "move_tenant",
    # use
    "resolve", "read", "record_run", "publish_model", "evaluate", "check_leakage",
    # find
    "search", "query", "lineage", "report",
    # stream
    "subscribe", "backfill", "poll",
    # simulation
    "time_passes", "fault",
}
REQUIRED_KEYS = {"id", "title", "variant", "given", "steps", "verifies", "gaps"}
MIN_EVALS_PER_USE_CASE = 5


def requirement_ids():
    text = (ROOT / "docs" / "requirements.md").read_text()
    return set(re.findall(r"^\| ([A-Z]{2}-\d+) \|", text, flags=re.M))


def main():
    known = requirement_ids()
    errors = []
    per_use_case = collections.Counter()
    variants = collections.defaultdict(set)
    used_requirements = collections.Counter()
    used_actions = collections.Counter()
    gaps = []
    seen_ids = set()

    for path in sorted(EVALS.glob("uc*.yaml")):
        try:
            evals = yaml.safe_load(path.read_text())
        except yaml.YAMLError as err:
            errors.append(f"{path.name}: not valid YAML: {' '.join(str(err).split())}")
            continue
        if not isinstance(evals, list):
            errors.append(f"{path.name}: the file must contain a list of evals")
            continue
        for ev in evals:
            eid = ev.get("id", "?")
            where = f"{path.name} {eid}"
            missing = REQUIRED_KEYS - ev.keys()
            if missing:
                errors.append(f"{where}: missing keys {sorted(missing)}")
                continue
            match = re.fullmatch(r"UC(\d+)-(\d{2})", eid)
            if not match:
                errors.append(f"{where}: the id must have the form UCn-nn")
                continue
            if eid in seen_ids:
                errors.append(f"{where}: the id is not unique")
            seen_ids.add(eid)
            use_case = int(match.group(1))
            per_use_case[use_case] += 1
            if ev["variant"] not in VARIANTS:
                errors.append(f"{where}: unknown variant {ev['variant']!r}")
            variants[use_case].add(ev["variant"])
            if not ev["given"]:
                errors.append(f"{where}: 'given' is empty")
            if not ev["steps"]:
                errors.append(f"{where}: 'steps' is empty")
            checks = 0
            for n, step in enumerate(ev["steps"], 1):
                for key in ("actor", "do", "with", "expect"):
                    if key not in step:
                        errors.append(f"{where} step {n}: missing {key!r}")
                for key, value in (step.get("with") or {}).items():
                    if value is None:
                        errors.append(f"{where} step {n}: parameter {key!r} has no value (a comma that needs quotes?)")
                action = step.get("do")
                if action not in ACTIONS:
                    errors.append(f"{where} step {n}: unknown action {action!r}")
                used_actions[action] += 1
                checks += len(step.get("expect") or [])
            if checks == 0:
                errors.append(f"{where}: no 'expect' statements")
            if not ev["verifies"]:
                errors.append(f"{where}: 'verifies' is empty")
            for rid in ev["verifies"]:
                if rid not in known:
                    errors.append(f"{where}: unknown requirement {rid}")
                used_requirements[rid] += 1
            for gap in ev["gaps"] or []:
                gaps.append((eid, gap))

    for use_case, count in sorted(per_use_case.items()):
        if count < MIN_EVALS_PER_USE_CASE:
            errors.append(f"use case {use_case}: {count} evals, need {MIN_EVALS_PER_USE_CASE}")

    total = sum(per_use_case.values())
    print(f"{total} evals in {len(per_use_case)} use cases")
    for use_case in sorted(per_use_case):
        kinds = ", ".join(sorted(variants[use_case]))
        print(f"  use case {use_case}: {per_use_case[use_case]} evals; variants: {kinds}")
    print(f"{len(used_requirements)} of {len(known)} requirements have at least one eval")
    unused_actions = sorted(ACTIONS - set(used_actions))
    if unused_actions:
        print(f"actions with no eval: {', '.join(unused_actions)}")
    areas = collections.Counter(r.split("-")[0] for r in known - set(used_requirements))
    print("requirements with no eval, by area: "
          + ", ".join(f"{a} {n}" for a, n in sorted(areas.items())))
    print(f"\n{len(gaps)} gaps:")
    for eid, gap in gaps:
        print(f"  {eid}: {gap}")

    if errors:
        print(f"\n{len(errors)} errors:")
        for err in errors:
            print(f"  {err}")
        return 1
    print("\nall eval files are valid")
    return 0


if __name__ == "__main__":
    sys.exit(main())

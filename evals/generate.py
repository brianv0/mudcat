#!/usr/bin/env python3
"""Expand the task templates into concrete task variants.

Each template in tasks.yaml has decision axes, cover axes, an oracle and
checks. This script makes the variants of each template, computes the expected
outcome of each variant with the oracle, and selects the checks that apply.

Decision axes: one variant for each combination of values.
Cover axes:    the values rotate through the variants. The script adds
               variants until each cover value occurs with each outcome.

Usage, from the repository root:
  python3 evals/generate.py               validate the templates, print a summary
  python3 evals/generate.py --show T3.1   print the variants of one template
  python3 evals/generate.py --pairs T3.1  print the minimal pairs of one template
  python3 evals/generate.py --write       write evals/generated/variants.jsonl
"""
import argparse
import collections
import itertools
import json
import pathlib
import re
import sys

import yaml

ROOT = pathlib.Path(__file__).resolve().parent.parent
EVALS = ROOT / "evals"
REQUIRED_KEYS = {
    "id", "use_case", "name", "goal", "axes", "fixture",
    "oracle", "checks", "invariants", "verifies", "reference",
}
OPTIONAL_KEYS = {"cover", "exclude", "distractors"}
CHECK_KEYS = {"check", "outcome", "when"}
MIN_VARIANTS = 5


def as_list(value):
    return value if isinstance(value, list) else [value]


def matches(condition, params):
    """True if each key of the condition has one of its values in params."""
    return all(params[key] in as_list(value) for key, value in condition.items())


def outcome_of(template, params):
    """The outcome of the first oracle rule that matches the parameters."""
    for rule in template["oracle"]:
        if "else" in rule:
            return rule["else"]["outcome"]
        if matches(rule["if"], params):
            return rule["then"]["outcome"]
    return None


def decision_combos(template):
    axes = template["axes"]
    for values in itertools.product(*axes.values()):
        params = dict(zip(axes, values))
        if not any(matches(ex, params) for ex in template.get("exclude", [])):
            yield params


def cover_values(cover, counter):
    """Values of the cover axes for a counter. The counter steps through all combinations."""
    values, stride = {}, 1
    for name, options in cover.items():
        values[name] = options[(counter // stride) % len(options)]
        stride *= len(options)
    return values


def parameter_sets(template):
    """Decision combinations with cover values, so that each cover value meets each outcome."""
    cover = template.get("cover", {})
    combos = list(decision_combos(template))
    sets = [{**combo, **cover_values(cover, n)} for n, combo in enumerate(combos)]
    if not cover:
        return sets
    by_outcome = collections.defaultdict(list)
    for combo in combos:
        by_outcome[outcome_of(template, combo)].append(combo)
    met = {(name, p[name], outcome_of(template, p)) for p in sets for name in cover}
    used = collections.Counter()
    counter = len(combos)
    for outcome, group in by_outcome.items():
        for name, options in cover.items():
            for option in options:
                if (name, option, outcome) in met:
                    continue
                combo = min(group, key=lambda c: used[tuple(c.values())])
                used[tuple(combo.values())] += 1
                params = {**combo, **cover_values(cover, counter), name: option}
                counter += 1
                if params not in sets:
                    sets.append(params)
                met.update((n, params[n], outcome) for n in cover)
    return sets


def variants(template):
    cover = list(template.get("cover", {}))
    for number, params in enumerate(parameter_sets(template), 1):
        outcome = outcome_of(template, params)
        checks = [
            c["check"].format(**params)
            for c in template["checks"]
            if ("outcome" not in c or outcome in as_list(c["outcome"]))
            and ("when" not in c or matches(c["when"], params))
        ]
        yield {
            "id": f"{template['id']}#{number:03d}",
            "template": template["id"],
            "use_case": template["use_case"],
            "goal": template["goal"],
            "params": params,
            "fixture": " ".join(template["fixture"].format(**params).split()),
            "distractors": template.get("distractors", []),
            "expected": outcome,
            "checks": checks,
            "same_outcome_for_each_value_of": cover,
            "invariants": template["invariants"],
            "verifies": template["verifies"],
        }


def minimal_pairs(template):
    """Pairs of decision combinations that differ in one axis and have different outcomes."""
    combos = list(decision_combos(template))
    for a, b in itertools.combinations(combos, 2):
        differ = [k for k in a if a[k] != b[k]]
        if len(differ) != 1:
            continue
        oa, ob = outcome_of(template, a), outcome_of(template, b)
        if oa != ob:
            yield differ[0], a, oa, b, ob


def validate_template(t, invariant_ids, requirement_ids, scenario_ids):
    tid = t.get("id", "?")
    errors = []
    missing = REQUIRED_KEYS - t.keys()
    if missing:
        return [f"{tid}: missing keys {sorted(missing)}"]
    unknown = t.keys() - REQUIRED_KEYS - OPTIONAL_KEYS
    if unknown:
        errors.append(f"{tid}: unknown keys {sorted(map(str, unknown))}")
    axes, cover = t["axes"], t.get("cover", {})
    both = set(axes) & set(cover)
    if both:
        errors.append(f"{tid}: {sorted(both)} are decision axes and cover axes")

    def check_condition(cond, where):
        for key, value in cond.items():
            if key in cover:
                errors.append(f"{tid} {where}: the cover axis {key!r} must not change the outcome or a check")
            elif key not in axes:
                errors.append(f"{tid} {where}: {key!r} is not an axis")
            else:
                for v in as_list(value):
                    if v not in axes[key]:
                        errors.append(f"{tid} {where}: {v!r} is not a value of the axis {key!r}")

    outcomes = set()
    for rule in t["oracle"]:
        if "else" in rule:
            outcomes.add(rule["else"]["outcome"])
        elif "if" in rule and "then" in rule:
            check_condition(rule["if"], "oracle")
            outcomes.add(rule["then"]["outcome"])
        else:
            errors.append(f"{tid} oracle: a rule needs 'if' and 'then', or 'else'")
    for ex in t.get("exclude", []):
        check_condition(ex, "exclude")
    for c in t["checks"]:
        extra = c.keys() - CHECK_KEYS
        if extra or "check" not in c:
            # PyYAML reads the keys on, off, yes and no as booleans
            errors.append(f"{tid} check: keys must be from {sorted(CHECK_KEYS)}, found {sorted(map(str, c))}")
            continue
        check_condition(c.get("when", {}), "check")
        for r in as_list(c.get("outcome", [])):
            if r not in outcomes:
                errors.append(f"{tid} check: {r!r} is not an outcome of the oracle")
    names = set(axes) | set(cover)
    for text in [t["fixture"]] + [c.get("check", "") for c in t["checks"]]:
        for placeholder in re.findall(r"\{(\w+)\}", text):
            if placeholder not in names:
                errors.append(f"{tid}: {placeholder!r} in braces is not an axis")
    for name in names:
        if "{" + name + "}" not in t["fixture"]:
            errors.append(f"{tid} fixture: the axis {name!r} is not in the fixture")
    for inv in t["invariants"]:
        if inv not in invariant_ids:
            errors.append(f"{tid}: unknown invariant {inv}")
    for rid in t["verifies"]:
        if rid not in requirement_ids:
            errors.append(f"{tid}: unknown requirement {rid}")
    if t["reference"] not in scenario_ids:
        errors.append(f"{tid}: unknown reference scenario {t['reference']}")
    if errors:
        return errors

    made = list(variants(t))
    if len(made) < MIN_VARIANTS:
        errors.append(f"{tid}: {len(made)} variants, need {MIN_VARIANTS}")
    for v in made:
        if v["expected"] is None:
            errors.append(f"{v['id']}: no oracle rule matches {v['params']}")
        if not v["checks"]:
            errors.append(f"{v['id']}: no check applies to {v['params']}")
    # each outcome of the oracle must occur, so that no rule is dead
    for r in outcomes - {v["expected"] for v in made}:
        errors.append(f"{tid}: no variant has the outcome {r!r}")
    # each oracle rule must decide at least one variant
    combos = list(decision_combos(t))
    for n, rule in enumerate(t["oracle"], 1):
        earlier = t["oracle"][: n - 1]
        decides = any(
            ("else" in rule or matches(rule["if"], p))
            and not any("if" in e and matches(e["if"], p) for e in earlier)
            for p in combos)
        if not decides:
            errors.append(f"{tid} oracle: rule {n} decides no variant")
    # each decision axis must change the outcome or the checks of some variant
    for name, values in axes.items():
        groups = collections.defaultdict(set)
        for p in combos:
            rest = tuple((k, str(x)) for k, x in p.items() if k != name)
            checks = tuple(
                c["check"] for c in t["checks"]
                if ("outcome" not in c or outcome_of(t, p) in as_list(c["outcome"]))
                and ("when" not in c or matches(c["when"], p)))
            groups[rest].add((outcome_of(t, p), checks))
        if all(len(g) == 1 for g in groups.values()):
            errors.append(f"{tid}: the axis {name!r} never changes the outcome or the checks. "
                          "Make it a cover axis or remove it")
    # each outcome must have a minimal pair, so that a constant answer fails
    if len(outcomes) > 1:
        in_pair = set()
        for _, _, oa, _, ob in minimal_pairs(t):
            in_pair.update((oa, ob))
        for r in outcomes - in_pair:
            errors.append(f"{tid}: the outcome {r!r} has no minimal pair")
    return errors


def load():
    templates = yaml.safe_load((EVALS / "tasks.yaml").read_text())
    invariants = yaml.safe_load((EVALS / "invariants.yaml").read_text())
    requirement_ids = set(re.findall(
        r"^\| ([A-Z]{2}-\d+) \|", (ROOT / "docs" / "requirements.md").read_text(), flags=re.M))
    scenario_ids = set()
    for path in (EVALS / "scenarios").glob("uc*.yaml"):
        scenario_ids.update(ev["id"] for ev in yaml.safe_load(path.read_text()))
    return templates, invariants, requirement_ids, scenario_ids


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--show", metavar="TEMPLATE", help="print the variants of one template")
    parser.add_argument("--pairs", metavar="TEMPLATE", help="print the minimal pairs of one template")
    parser.add_argument("--write", action="store_true", help="write evals/generated/variants.jsonl")
    args = parser.parse_args(argv)

    templates, invariants, requirement_ids, scenario_ids = load()
    errors = []
    invariant_ids = {inv["id"] for inv in invariants}
    for inv in invariants:
        for rid in inv["verifies"]:
            if rid not in requirement_ids:
                errors.append(f"{inv['id']}: unknown requirement {rid}")
    seen = set()
    valid = []
    for t in templates:
        if t.get("id") in seen:
            errors.append(f"{t.get('id')}: the id is not unique")
        seen.add(t.get("id"))
        found = validate_template(t, invariant_ids, requirement_ids, scenario_ids)
        errors.extend(found)
        if not found:
            valid.append(t)
    made = [v for t in valid for v in variants(t)]

    if args.show:
        for v in made:
            if v["template"] == args.show:
                print(f"{v['id']}  {v['params']}\n  expected: {v['expected']}")
                for c in v["checks"]:
                    print(f"    - {c}")
    elif args.pairs:
        for t in valid:
            if t["id"] == args.pairs:
                for axis, a, oa, b, ob in minimal_pairs(t):
                    same = {k: v for k, v in a.items() if k != axis}
                    print(f"{axis}: {a[axis]} -> {oa} | {b[axis]} -> {ob}   with {same}")
    else:
        print(f"{len(templates)} task templates, {len(made)} variants, {len(invariants)} invariants")
        for t in valid:
            mine = [v for v in made if v["template"] == t["id"]]
            spread = collections.Counter(v["expected"] for v in mine)
            decision = " x ".join(f"{n}({len(v)})" for n, v in t["axes"].items())
            cover = ", ".join(f"{n}({len(v)})" for n, v in t.get("cover", {}).items())
            pairs = sum(1 for _ in minimal_pairs(t))
            print(f"  {t['id']} {t['name']}: {len(mine)} variants, {pairs} minimal pairs")
            print(f"      decision: {decision}" + (f"; cover: {cover}" if cover else ""))
            print("      outcomes: " + ", ".join(f"{r} {n}" for r, n in spread.most_common()))
        per_use_case = collections.Counter(v["use_case"] for v in made)
        print("variants per use case: "
              + ", ".join(f"{uc}: {n}" for uc, n in sorted(per_use_case.items())))
        used = {rid for t in valid for rid in t["verifies"]}
        used |= {rid for inv in invariants for rid in inv["verifies"]}
        print(f"{len(used)} of {len(requirement_ids)} requirements have a task or an invariant")

    if args.write:
        out = EVALS / "generated"
        out.mkdir(exist_ok=True)
        with (out / "variants.jsonl").open("w") as f:
            for v in made:
                f.write(json.dumps(v) + "\n")
        print(f"wrote {len(made)} variants to {out / 'variants.jsonl'}")

    if errors:
        print(f"\n{len(errors)} errors:")
        for err in errors:
            print(f"  {err}")
        return 1
    if not (args.show or args.pairs):
        print("\nall task templates are valid")
    return 0


if __name__ == "__main__":
    sys.exit(main())

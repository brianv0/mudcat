#!/usr/bin/env python3
"""Report sentences that do not follow ASD Simplified Technical English.

For each Markdown file: sentences of more than 25 words, passive voice (a
heuristic), words that end in -ing, and some words that are not approved.
The list of words is from memory of ASD-STE100. It is indicative only.

Usage: python3 tools/ste_check.py docs/*.md README.md
"""
import re, sys, collections
IRREG = set("done made given taken written known shown seen kept held built sent set put run read found left lost bound brought chosen drawn driven frozen hidden meant paid split spent told thought understood won broken".split())
UNAPPROVED = {  # indicative only; from memory of ASD-STE100, not checked against the dictionary
 'ensure':'make sure','utilize':'use','utilise':'use','via':'through','leverage':'use','approximately':'about',
 'obtain':'get','obtains':'get','require':'need','requires':'need','required':'necessary','requiring':'need',
 'whether':'if','therefore':'thus / as a result','however':'but','commence':'start','prior':'before',
 'sufficient':'enough','additional':'more','numerous':'many','facilitate':'help','perform':'do','performs':'do',
 'enable':'let','enables':'let','allows':'lets','allow':'let','indicate':'show','indicates':'shows',
 'regarding':'about','whereas':'but','thereby':'thus','typically':'usually','currently':'now'}
TECH_ING = {'string','during','thing','nothing','something','anything','everything','ring','king','spring','ceiling'}
def prose(text):
    text = re.sub(r'```.*?```', '', text, flags=re.S)
    out=[]
    for line in text.splitlines():
        l=line.strip()
        if not l or l.startswith('#') or l.startswith('|'): continue
        l=re.sub(r'^[-*\d.]+\s+','',l)
        l=re.sub(r'`[^`]*`','X',l); l=re.sub(r'\[([^\]]*)\]\([^)]*\)',r'\1',l); l=l.replace('**','')
        out.append(l)
    return out
def sentences(lines):
    s=[]
    for l in lines:
        for x in re.split(r'(?<=[.!?])\s+(?=[A-Z(])', l):
            if len(x.split())>=3: s.append(x)
    return s
tot=collections.Counter(); words_all=collections.Counter()
for f in sys.argv[1:]:
    txt=open(f).read(); sents=sentences(prose(txt))
    lens=[len(x.split()) for x in sents]
    long_=[x for x in sents if len(x.split())>25]
    passive=[x for x in sents if re.search(r'\b(is|are|was|were|be|been|being)\s+(\w+ly\s+)?(\w+ed|'+'|'.join(IRREG)+r')\b', x, re.I)]
    ing=[w for x in sents for w in re.findall(r'\b[a-z]+ing\b', x) if w not in TECH_ING]
    un=collections.Counter(w for x in sents for w in re.findall(r"[a-z]+", x.lower()) if w in UNAPPROVED)
    words_all.update(un)
    n=len(sents)
    print(f"\n== {f.split('/')[-1]}: {n} sentences, mean {sum(lens)/n:.1f} words, max {max(lens)}")
    print(f"  >25 words: {len(long_)} ({100*len(long_)/n:.0f}%)   passive (heuristic): {len(passive)} ({100*len(passive)/n:.0f}%)   -ing words: {len(ing)}")
    print("  top -ing:", collections.Counter(ing).most_common(8))
    print("  indicative unapproved words:", un.most_common(10))
    for x in sorted(long_, key=lambda x:-len(x.split()))[:2]: print("  longest:", len(x.split()), "|", x[:200])
print("\nAll files, indicative unapproved words:", words_all.most_common(15))

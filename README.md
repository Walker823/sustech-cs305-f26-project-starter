# SUSTech CS305 2026 Fall Project Starter

For SUSTech CS305 Fall 2026: Adaptive Multi-CDN Content Delivery

## Important Updates

- **[2026-11-02]**
  - We released the CS305 2026 Fall Project: assignment, interface reference, framework,
    starter skeleton, public tests, fault proxy.

These are the important updates. For full information, see [CHANGELOG.md](CHANGELOG.md).

## Warning

If you **fork** this repository, the new repository is **public** by default.

- **Risk:** any code you push to a public fork is visible to other students, and to next year's class.
- **Responsibility:** keep your own code private until the end of the semester. Develop in a private
  repository, and do not upload your solution to a public gist, paste service or model playground.
- Publishing your own implementation, or another team's, is treated the same way as copying between
  teams: as academic misconduct.

## Introduction

Read these two documents before you write code:

- [`docs/Specification.pdf`](docs/Specification.pdf) — the assignment: components, requirements,
  scoring, submission and defense, collaboration and academic integrity.
- [`docs/Protocol.pdf`](docs/Protocol.pdf) — the interface reference: configuration fields, HTTP
  endpoints, headers, error codes, events, and the submission format (Section 8).

Then look at the code:

- `student/` — the modules you implement. Every `TODO` is yours; the file boundaries and the
  signatures are fixed by the interface reference.
- `framework/` — course-provided infrastructure: Origin, CDN and client runtimes, data helpers. Read
  it, do not modify it; the graded run uses its own copy.
- `tools/` — public tests, fault proxy and the submission packager.

Where this file and the two PDFs disagree, the PDFs govern.

## Key facts

| Item | Value |
| --- | --- |
| Release date | 2026-11-02 |
| Team size | 2–3 students; teammates may come from different lab sections |
| Environment | Python 3.11 or later (grading uses 3.13), standard library only, Linux; on Windows use WSL2 |
| Submission | one ZIP per team, through the course OJ |
| Deadline | end of Week 15, Friday 23:59 (UTC+8) |
| Defense | Week 16 lab session, slides submitted beforehand; no written report |

The deadline is the one in the Specification. The OJ page is the submission channel; if it ever
displays a different date, follow the Specification and report the discrepancy to the teaching team.

## Run the public tests

```bash
python3 tools/public_tests.py --skeleton    # the untouched starter must at least run its lifecycle
python3 tools/public_tests.py               # all public functional samples
python3 tools/public_tests.py --only multi_source
```

Each sample starts a real Origin, CDN and Client in local processes, so a passing sample means the
mechanism works locally. These samples are not the hidden tests and they do not produce a score.

The `Dockerfile` is optional, for running the starter without a local Python setup:

```bash
docker build -t cs305-starter . && docker run --rm cs305-starter
```

## Inject faults yourself

`tools/fault_proxy.py` sits between your client and a CDN (or between your CDN and the Origin) and
applies latency, bandwidth, corruption, truncation, stalls, 5xx and resets from a JSON file:

```bash
python3 tools/fault_proxy.py --upstream http://127.0.0.1:9001 --faults faults.json --log proxy.jsonl
```

It prints one `ready` line with its port; point `cdns[].url` at that port. The module docstring lists
every rule. Testing concurrency, corrupt chunks, timeouts and output protection on failure is part of
the assignment, not an optional extra.

## Package and submit

```bash
python3 tools/package_submission.py --output submission.zip
```

Submit that ZIP through the course OJ. The OJ is the only submission channel — do not open pull
requests against this repository, and do not send code by email or chat.

## Team, defense, integrity

- One team submits one project. Every member should be able to explain the whole design.
- The defense is in the Week 16 lab session. Teams may attend a section other than their own; submit
  your slides before the defense. No written report is required.
- If the contribution of the members is not equal, mark each member's contribution ratio in the slides.
- Collaboration inside a team is expected. Code must be written by your own team; **copying or sharing
  code between teams is treated as academic misconduct.**

## Getting Help

**Start early.** The TAs may not be able to answer immediately, especially near the end of the semester.

So that every answer helps the whole class, ask project-related questions as
[**issues in this repository**](../../issues). Search the existing issues, open and closed, before
posting; most questions have already been asked.

- **Before posting:** search existing issues.
- **When posting:** give a clear title, the exact command you ran, the full output, the `faults.json`
  you used, and the smallest snippet of your own code that shows the problem.
- Do not paste complete implementations, and do not post code that is not your team's.
- Only project-related questions are answered. General questions about the course material belong in
  the lecture or the lab.
- Questions about grades or personal circumstances go to the teaching team directly, not here.

## Verify your checkout

`PUBLIC-MANIFEST.json` lists the SHA-256 of every starter file:

```bash
python3 - <<'PY'
import hashlib, json, pathlib
manifest = json.loads(pathlib.Path("PUBLIC-MANIFEST.json").read_text())
bad = [n for n, h in manifest.items()
       if hashlib.sha256(pathlib.Path(n).read_bytes()).hexdigest() != h]
print(len(manifest), "files match" if not bad else "MISMATCH: " + ", ".join(bad))
PY
```

If it reports a mismatch right after cloning on Windows, Git rewrote your line endings; `.gitattributes`
already forces LF, so check `git config core.autocrlf` before you edit anything.

## Repository policy

The PDFs are the source of truth for requirements. Corrections and clarifications are announced under
**Important Updates** above and in `CHANGELOG.md`, with a dated entry; the repository content always
matches `PUBLIC-MANIFEST.json`. Do not edit the repository contents locally and expect grading to see
them — grading only sees the ZIP you submit.

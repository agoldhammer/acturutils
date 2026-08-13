# acturutils

Rust utilities for the `actur` article database.

## catcount

Counts articles in each category between two dates.

```
cargo build --release
./target/release/catcount --start 2026-08-01 --end 2026-08-07
```

```
Articles by category, 2026-08-01 00:00:00 .. 2026-08-07 23:59:59 UTC

CATEGORY                ARTICLES      PCT
---------------------  ---------  -------
Other                       1357   19.58%
Sports                       582    8.40%
US Politics                  494    7.13%
...
---------------------  ---------  -------
TOTAL                       6929
```

### The ssh tunnel

`mongod` on con1 binds to `127.0.0.1:27017` only, so it is unreachable from
outside the box. `catcount` therefore spawns its own tunnel

```
ssh -N -L 27017:127.0.0.1:27017 con1
```

on startup and kills it on exit. Nothing needs to be running beforehand. If
something is already listening on the local port (an existing tunnel, or a local
`mongod`), that endpoint is reused instead and a note is printed to stderr.

Use `--no-tunnel` to talk to a mongod directly, `--ssh-host` for a different
host, and `--local-port` if 27017 is taken locally.

### Dates

`--start` / `--end` accept `YYYY-MM-DD`, `YYYY-MM-DD HH:MM[:SS]`, or RFC 3339,
and are interpreted as **UTC** — `pubdate` is stored as a naive datetime built
from the feed's parsed time tuple, with UTC assumed throughout `actur`.

The range is inclusive on both ends: a bare `--start` date means `00:00:00` of
that day and a bare `--end` date means `23:59:59.999`, so
`--start 2026-01-01 --end 2026-01-31` covers all of January.

### Category normalization

The `cat` field holds the OpenAI reply verbatim, with no validation, so the
collection contains well over a thousand distinct labels — including variants
that differ only by a trailing period, stray whitespace, or case
(`International Affairs`, `International Affairs.`, `International affairs`).

`--normalize` merges those variants. Each bucket is reported under its most
common original spelling, so acronyms survive (`US Politics`, not
`Us Politics`). Genuinely distinct synonyms (`Tech` vs `Technology`, `Sports`
vs `Sport`) are **not** merged — that would need a hand-written mapping.

Articles with no `cat` field are counted under `(none)`.

### Breaking down by publication

`--by-pubname` splits each category's count across the 14 publications, sorted
by size, with each publication's share **of its category**:

```
CATEGORY           ARTICLES      PCT
----------------  ---------  -------
US Politics             107    9.44%
    NYTimes              26   24.30%
    Guardian             19   17.76%
    FT                   13   12.15%
    WSJ                  10    9.35%
...
```

The category rows keep their share of the grand total, so the two percentage
columns answer different questions: how big is this category, and who is
writing it.

In `csv` the output becomes one `category,pubname,count` row per pair, ready to
pivot; in `json` each category gains a `publications` array. Publications
contributing nothing to a category are omitted rather than listed as zero.

### Options

| Flag | Meaning |
| --- | --- |
| `-s, --start`, `-e, --end` | Range bounds (required) |
| `--by-pubname` | Break each category's count down by publication |
| `--normalize` | Merge whitespace/period/case variants of a label |
| `--top N` | Show only the N largest categories |
| `--min N` | Hide categories with fewer than N articles |
| `--format table\|csv\|json` | Output format (default `table`) |
| `--no-tunnel` | Connect directly instead of tunnelling |
| `--ssh-host`, `--local-port`, `--remote-port` | Tunnel settings (default `con1`, 27017, 27017) |
| `--uri`, `--db`, `--collection` | Mongo overrides (default `actur`, `articles`) |

`--top` and `--min` select *categories* and only trim the displayed rows; the
publications within a retained category are always shown in full, and `TOTAL`
always reflects every article in the range.

### Notes

Counting is done server-side with a `$match` + `$group` aggregation, using the
existing `pubdate` descending index, so it does not pull documents over the
tunnel. A full-year query over ~1.27M articles returns in a couple of seconds.

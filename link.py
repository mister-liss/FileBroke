#!/usr/bin/env python3
"""
link.py — AMC-first linker (FileBot) with manual Movie/TV fallback (hardlink)
with verbose, per-file banners and a preflight prompt.

- Logs (banners, AMC summaries, prompts) -> STDERR
- Machine results (per-file JSON) -> STDOUT

Typical usage:
  python3 compare_3.py --src /mnt/nas/media/Transmission/Downloads --dst /mnt/nas/media/Library --format jsonl \
  | python3 link_4.py --library-root /mnt/nas/media/Library --interactive --non-strict --verbose

Flags:
  --interactive    Ask per-file whether to process (y/n/a/i/q/p).
  --non-strict     Pass -non-strict to FileBot AMC.
  --amc-def        Extra --def KEY=VAL for AMC (repeatable).
  --fallback-copy  If hardlink is blocked (EPERM/EXDEV/EACCES), copy instead of failing.
  --verbose        Print AMC command and a tail of AMC output to STDERR.
"""
from __future__ import annotations
import argparse, errno, json, os, re, shutil, subprocess, sys
from pathlib import Path
from typing import Dict, Any, List, Optional

# -------------------- logging & io --------------------
def log(msg: str = ""):
    sys.stderr.write(msg + "\n"); sys.stderr.flush()

def json_out(obj: Dict[str, Any]):
    sys.stdout.write(json.dumps(obj, default=str) + "\n"); sys.stdout.flush()

def read_choice_from_tty(prompt: str) -> str:
    try:
        with open("/dev/tty", "r") as tty:
            sys.stderr.write(prompt); sys.stderr.flush()
            return tty.readline().strip()
    except Exception:
        try:
            return input(prompt).strip()
        except EOFError:
            return ""

def prompt_work_on_file(src: Path) -> str:
    sys.stderr.write(
        "\n[y] process  [n] skip  [a] process ALL  [i] ignore ALL  [q] quit  [p] print path\n"
    ); sys.stderr.flush()
    while True:
        c = (read_choice_from_tty("Choice (y/n/a/i/q/p): ") or "").strip().lower()
        if c in {"y","n","a","i","q","p"}:
            return c
        if c == "":
            return "n"

# -------------------- link & subs helpers --------------------
def hardlink_atomic(src: Path, dst: Path, fallback_copy: bool = False):
    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.parent / (".tmp_link_" + dst.name)
    if tmp.exists() or tmp.is_symlink():
        try: tmp.unlink()
        except Exception: pass
    try:
        os.link(str(src), str(tmp))
    except OSError as e:
        # Fall back to copy if requested (EPERM/EXDEV/EACCES are the typical blockers)
        if fallback_copy and e.errno in (getattr(errno, "EPERM", 1),
                                         getattr(errno, "EXDEV", 18),
                                         getattr(errno, "EACCES", 13)):
            shutil.copy2(str(src), str(tmp))
        else:
            raise
    os.replace(str(tmp), str(dst))

SUB_EXTS = {".srt", ".sub", ".ass", ".vtt", ".idx"}

def subtitle_dest_name_for_movie(movie_basename: str, sub_path: Path) -> str:
    lower_orig = sub_path.name.lower()
    lower_movie = movie_basename.lower().replace(" ", ".")
    idx = lower_orig.find(lower_movie)
    if idx != -1:
        tail = sub_path.name[idx + len(lower_movie):]
        if tail.startswith((".", " ", "-")) or tail == "":
            return f"{movie_basename}{tail}"
    parts = re.split(r"[._\s-]+", sub_path.stem)
    suffix = []
    for p in reversed(parts):
        p2 = p.strip().lower()
        if not p2: continue
        if re.fullmatch(r"[a-z]{2,3}(?:-[A-Z]{2})?", p2) or p2 in {"forced", "sdh", "cc", "hi"}:
            suffix.insert(0, p2)
        else:
            break
    return movie_basename + ("." + ".".join(suffix) if suffix else "") + sub_path.suffix

def link_subtitles(src_video: Path, dst_video: Path, subtitles: List[str],
                   fallback_copy: bool, verbose: bool = False):
    out = []
    base = dst_video.stem
    for s in subtitles or []:
        sp = Path(s)
        try:
            if sp.suffix.lower() not in SUB_EXTS:
                continue
            dest_name = subtitle_dest_name_for_movie(base, sp)
            dest_path = dst_video.parent / dest_name
            if dest_path.exists() or dest_path.is_symlink():
                out.append({"src": str(sp), "action": "skipped-exists"})
                continue
            hardlink_atomic(sp, dest_path, fallback_copy=fallback_copy)
            out.append({"src": str(sp), "dst": str(dest_path), "action": "linked"})
            if verbose: log(f"[subs] linked: {sp} -> {dest_path}")
        except Exception as e:
            if verbose: log(f"[subs] failed {sp}: {e}")
            out.append({"src": str(sp), "action": "failed", "error": str(e)})
    return out

# -------------------- FileBot AMC --------------------
AMC_MOVE_RE = re.compile(r'(?i)\b(?:Move|Rename|Copy|Link)\b.*?\b(?:to|into)\b\s+["\']?([^"\']+)["\']?$')

def run_filebot_amc(src: Path, output_root: Path, action: str, non_strict: bool,
                    extra_defs: List[str], verbose: bool = False):
    cmd = ["filebot", "-script", "fn:amc",
           "--action", action,
           "--output", str(output_root),
           "--conflict", "auto"]
    if non_strict:
        cmd.append("-non-strict")
    defs = ["clean=n", "artwork=n", "music=n"]
    defs.extend(extra_defs or [])
    for d in defs:
        cmd += ["--def", d]
    cmd.append(str(src))
    if verbose: log("[filebot] " + " ".join(cmd))
    try:
        p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, check=False)
        return p.returncode, p.stdout or ""
    except FileNotFoundError:
        return 127, "filebot-not-found"
    except Exception as e:
        return 2, str(e)

def parse_amc_destinations(amc_stdout: str) -> List[str]:
    dests = []
    for line in (amc_stdout or "").splitlines():
        m = AMC_MOVE_RE.search(line.strip())
        if m:
            dests.append(m.group(1).strip())
    # de-dup preserving order
    seen = set(); uniq = []
    for d in dests:
        if d not in seen:
            uniq.append(d); seen.add(d)
    return uniq

# -------------------- Manual prompts --------------------
def manual_movie(movies_root: Path, src: Path) -> Optional[Path]:
    log("\nManual: Movie")
    title = read_choice_from_tty("  Title: ").strip()
    if not title:
        log("  Title required."); return None
    year = read_choice_from_tty("  Year (YYYY): ").strip()
    if not re.fullmatch(r"\d{4}", year):
        log("  Valid 4-digit year required."); return None
    tmdb = read_choice_from_tty("  TMDB id (optional digits; Enter to skip): ").strip()
    tmdb_val = f"tmdb-{tmdb}" if re.fullmatch(r"\d+", tmdb) else "tmdb-unknown"
    folder = f"{title} ({year}) - " + "{" + tmdb_val + "}"
    dst_dir = movies_root / folder
    dst_name = f"{title} ({year}){src.suffix}"
    preview = dst_dir / dst_name
    log(f"  -> {preview}")
    return preview

def manual_tv(tv_root: Path, src: Path) -> Optional[Path]:
    log("\nManual: TV")
    series = read_choice_from_tty("  Series Title: ").strip()
    if not series:
        log("  Series Title required."); return None
    s = read_choice_from_tty("  Season #: ").strip()
    e = read_choice_from_tty("  Episode #: ").strip()
    if not (s.isdigit() and e.isdigit()):
        log("  Season/Episode must be integers."); return None
    tmdb = read_choice_from_tty("  TMDB series id (optional digits; Enter to skip): ").strip()
    tmdb_tag = f" - {{tmdb-{tmdb}}}" if re.fullmatch(r"\d+", tmdb) else ""
    dst_dir = tv_root / f"{series}{tmdb_tag}" / f"Season {int(s):02d}"
    dst_name = f"{series} - s{int(s):02d}e{int(e):02d}{src.suffix}"
    preview = dst_dir / dst_name
    log(f"  -> {preview}")
    return preview

# -------------------- JSONL input --------------------
def read_jsonl_records(jsonl_file: Optional[Path]) -> List[Dict[str, Any]]:
    recs = []
    if jsonl_file:
        with open(jsonl_file, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line: continue
                try: recs.append(json.loads(line))
                except Exception: log(f"Skipping invalid JSON line: {line}")
    else:
        for line in sys.stdin:
            line = line.strip()
            if not line: continue
            try: recs.append(json.loads(line))
            except Exception: log(f"Skipping invalid JSON line: {line}")
    return recs

# -------------------- main --------------------
def main(argv=None):
    ap = argparse.ArgumentParser(description="AMC-first linker with verbose per-file output and preflight prompt.")
    ap.add_argument("--library-root", required=True, help="Library root containing 'Movies' and 'TV Shows'.")
    ap.add_argument("--interactive", action="store_true", help="Ask before linking.")
    ap.add_argument("--fallback-copy", action="store_true", help="On EPERM/EXDEV, copy instead of failing.")
    ap.add_argument("--non-strict", action="store_true", help="Pass -non-strict to FileBot AMC.")
    ap.add_argument("--amc-def", action="append", default=[], help="Extra --def KEY=VAL for AMC (repeatable).")
    ap.add_argument("--verbose", action="store_true")
    ap.add_argument("jsonl_file", nargs="?", help="Optional JSONL file; else read stdin.")
    args = ap.parse_args(argv)

    library_root = Path(args.library_root)
    movies_root  = library_root / "Movies"
    tv_root      = library_root / "TV Shows"
    for p in (library_root, movies_root, tv_root):
        if not p.exists() or not p.is_dir():
            log(f"Path must exist and be a directory: {p}"); return 2

    inputs = read_jsonl_records(Path(args.jsonl_file) if args.jsonl_file else None)
    if not inputs:
        if args.verbose: log("No input records.")
        return 0

    # persistent interactive choices
    process_all = False
    ignore_all = False

    total = len(inputs)
    for idx, rec in enumerate(inputs, start=1):
        src = Path(rec.get("src", ""))
        banner = "=" * 80
        log(f"\n{banner}\n[{idx}/{total}] SRC: {src}")
        if not src.exists() or not src.is_file():
            json_out({"src": str(src), "ok": False, "action": "missing-src"})
            log("  -> missing source; skipped")
            continue

        # Preflight confirmation (interactive)
        if args.interactive:
            if ignore_all:
                json_out({"src": str(src), "ok": False, "action": "ignored-all"})
                log("  -> ignored-all")
                continue
            if not process_all:
                ch = prompt_work_on_file(src)
                if ch == "p":
                    log(f"  PATH: {src}")
                    ch = prompt_work_on_file(src)
                if ch == "q":
                    json_out({"src": str(src), "ok": False, "action": "aborted-by-user"})
                    log("  -> aborted-by-user")
                    return 130
                if ch == "i":
                    ignore_all = True
                    json_out({"src": str(src), "ok": False, "action": "ignored-all"})
                    log("  -> ignored-all")
                    continue
                if ch == "n":
                    json_out({"src": str(src), "ok": False, "action": "skipped-by-user"})
                    log("  -> skipped-by-user")
                    continue
                if ch == "a":
                    process_all = True
                # 'y' falls through

        # 1) AMC preview
        rc_test, out_test = run_filebot_amc(src, library_root, action="test",
                                            non_strict=args.non_strict, extra_defs=args.amc_def,
                                            verbose=args.verbose)
        if args.verbose:
            log(f"[AMC test rc] {rc_test}")
            if out_test:
                lines = out_test.splitlines()
                preview = "\n".join(lines[-50:]) if len(lines) > 50 else out_test
                log(preview)

        dests = parse_amc_destinations(out_test) if rc_test == 0 else []
        if rc_test == 0 and dests:
            log("Proposed destinations:")
            for d in dests:
                log(f"  -> {d}")
        elif rc_test == 0 and not dests:
            if re.search(r'No files selected for processing', out_test or "", re.I):
                log("AMC matched run but selected 0 files (likely considered 'extras').")
            else:
                log("AMC returned success but no destinations parsed.")
        else:
            if re.search(r'Ignore extra:', out_test or "", re.I):
                log("AMC ignored as extra.")
            else:
                log("AMC did not recognize this item.")

        accept_amc = False
        if rc_test == 0 and dests:
            if args.interactive:
                ch = (read_choice_from_tty("Use FileBot AMC to hardlink? (y/N): ") or "n").lower()
                accept_amc = ch in ("y", "yes")
            else:
                accept_amc = True

        if accept_amc:
            rc_link, out_link = run_filebot_amc(src, library_root, action="hardlink",
                                                non_strict=args.non_strict, extra_defs=args.amc_def,
                                                verbose=args.verbose)
            ok = (rc_link == 0)
            json_out({"src": str(src), "ok": ok, "action": "filebot-hardlink"})
            log("  -> linked via FileBot" if ok else "  -> FileBot hardlink FAILED")
            if args.verbose and out_link:
                lines = out_link.splitlines()
                preview = "\n".join(lines[-50:]) if len(lines) > 50 else out_link
                log(preview)
            continue

        # 2) Manual fallback
        log("FileBot couldn't (or declined). Manual mode.")
        mt = (read_choice_from_tty("Is this a (m)ovie or (t)v episode? [m/t]: ").strip().lower() or "m")
        dst_path: Optional[Path] = None
        if mt.startswith("m"):
            dst_path = manual_movie(movies_root, src)
        else:
            dst_path = manual_tv(tv_root, src)

        if not dst_path:
            json_out({"src": str(src), "ok": False, "action": "manual-aborted"})
            log("  -> manual aborted")
            continue

        if dst_path.exists():
            json_out({"src": str(src), "ok": True, "action": "exists", "dst_path": str(dst_path)})
            log("  -> already exists; skipped")
            continue

        try:
            hardlink_atomic(src, dst_path, fallback_copy=args.fallback_copy)
            subs_result = link_subtitles(src, dst_path, rec.get("subtitles", []),
                                         fallback_copy=args.fallback_copy, verbose=args.verbose)
            json_out({"src": str(src), "ok": True, "action": "linked", "dst_path": str(dst_path), "subs": subs_result})
            log(f"  -> linked to {dst_path}")
        except Exception as e:
            json_out({"src": str(src), "ok": False, "action": "failed", "error": str(e), "dst_path": str(dst_path)})
            log(f"  -> FAILED: {e}")

    return 0

if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        log("Interrupted"); sys.exit(130)

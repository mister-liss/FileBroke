#!/usr/bin/env python3
"""
Compare video files (by extension + min size) between two trees and emit machine-friendly
missing-file records.

- Emits JSON Lines when run with --format jsonl (one JSON object per missing file).
- Includes subtitle candidates found near each source file.
- Uses an SQLite cache to avoid re-hashing unchanged files.
- Default hashing is partial (first+last chunk) with blake2b.

Usage examples:
  # JSONL output for pipelines:
  python3 compare_3.py --src /transmission/Downloads --dst /plex/Media --format jsonl

  # Human-readable:
  python3 compare_3.py --src /transmission/Downloads --dst /plex/Media

  # Pipe JSONL into a consumer:
  python3 compare_3.py --src /transmission/Downloads --dst /plex/Media --format jsonl \
    | python3 consume_missing_jsonl.py
"""
from __future__ import annotations
import argparse
import hashlib
import json
import os
import sqlite3
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, List

BUF_SIZE = 1024 * 1024
DEFAULT_CACHE = Path(os.getenv("XDG_CACHE_HOME", Path.home() / ".cache")) / "largest_file_hashes.db"

# Subtitles we’ll try to link alongside videos
SUB_EXTS = {".srt", ".sub", ".ass", ".vtt", ".idx"}

# Video extensions to consider (lowercase)
VIDEO_EXTS = {
    ".mkv", ".mp4", ".m4v", ".mov", ".avi", ".wmv",
    ".ts", ".m2ts", ".mts", ".webm"
}

@dataclass
class FileInfo:
    path: Path
    size: int

# ------------------ cache ------------------
class HashCache:
    def __init__(self, path: Path):
        self.path = path
        self._conn = sqlite3.connect(str(self.path))
        self._ensure_table()

    def _ensure_table(self):
        cur = self._conn.cursor()
        cur.execute(
            """CREATE TABLE IF NOT EXISTS filehash
               (path TEXT PRIMARY KEY, mtime REAL, size INTEGER, method TEXT, algo TEXT, chunk_size INTEGER, hash TEXT)"""
        )
        self._conn.commit()

    def get(self, path: Path, mtime: float, size: int, method: str, algo: str, chunk_size: int) -> Optional[str]:
        cur = self._conn.cursor()
        cur.execute(
            "SELECT hash FROM filehash WHERE path=? AND mtime=? AND size=? AND method=? AND algo=? AND chunk_size=?",
            (str(path), mtime, size, method, algo, chunk_size),
        )
        r = cur.fetchone()
        return r[0] if r else None

    def set(self, path: Path, mtime: float, size: int, method: str, algo: str, chunk_size: int, digest: str) -> None:
        cur = self._conn.cursor()
        cur.execute(
            "REPLACE INTO filehash (path, mtime, size, method, algo, chunk_size, hash) VALUES (?,?,?,?,?,?,?)",
            (str(path), mtime, size, method, algo, chunk_size, digest),
        )
        self._conn.commit()

    def close(self):
        self._conn.close()

# ------------------ discovery ------------------
def parse_size(s: str) -> int:
    s = s.strip().lower()
    units = {"k": 1024, "m": 1024**2, "g": 1024**3, "t": 1024**4}
    if s[-1:].isdigit():
        return int(s)
    if s[-1] in units:
        return int(float(s[:-1]) * units[s[-1]])
    raise argparse.ArgumentTypeError(f"Invalid size: {s}")

def collect_video_files(root: Path, min_size: int, follow_symlinks: bool) -> Dict[Path, FileInfo]:
    """Return all video files (by extension) >= min_size under root."""
    files: Dict[Path, FileInfo] = {}
    for dirpath, dirnames, filenames in os.walk(root, followlinks=follow_symlinks):
        d = Path(dirpath)
        for fname in filenames:
            p = d / fname
            try:
                st = p.stat()
            except (OSError, PermissionError):
                continue
            if not p.is_file():
                continue
            if st.st_size < min_size:
                continue
            if p.suffix.lower() not in VIDEO_EXTS:
                continue
            files[p] = FileInfo(p, st.st_size)
    return files

# ------------------ hashing (full / partial) ------------------
def _hash_stream(path: Path, algo_name: str) -> str:
    h = hashlib.new(algo_name)
    with path.open("rb") as f:
        while True:
            chunk = f.read(BUF_SIZE)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()

def _hash_partial(path: Path, algo_name: str, chunk_size: int) -> str:
    size = path.stat().st_size
    if size <= chunk_size * 2:
        return _hash_stream(path, algo_name)
    h = hashlib.new(algo_name)
    with path.open("rb") as f:
        first = f.read(chunk_size)
        h.update(b"FIRST")
        h.update(first)
        f.seek(max(0, size - chunk_size))
        last = f.read(chunk_size)
        h.update(b"LAST")
        h.update(last)
    return h.hexdigest()

def hash_file(path: Path, algo: str = "blake2b", method: str = "partial", chunk_size: int = 4 * 1024 * 1024) -> str:
    if method == "full":
        return _hash_stream(path, algo)
    else:
        return _hash_partial(path, algo, chunk_size)

def hash_map(files: Dict[Path, FileInfo], algo: str, method: str, chunk_size: int, workers: int, cache: Optional[HashCache]) -> Dict[str, FileInfo]:
    out: Dict[str, FileInfo] = {}
    to_hash = []
    for fi in files.values():
        path = fi.path
        try:
            st = path.stat()
        except Exception:
            continue
        mtime = st.st_mtime
        size = st.st_size
        cached = None
        if cache:
            cached = cache.get(path, mtime, size, method, algo, chunk_size)
        if cached:
            out[cached] = fi
        else:
            to_hash.append(fi)
    if not to_hash:
        return out
    with ThreadPoolExecutor(max_workers=workers) as ex:
        fut_to_fi = {ex.submit(hash_file, fi.path, algo, method, chunk_size): fi for fi in to_hash}
        for fut in as_completed(fut_to_fi):
            fi = fut_to_fi[fut]
            try:
                digest = fut.result()
            except Exception as e:
                print(f"[hash error] {fi.path}: {e}", file=sys.stderr)
                continue
            out[digest] = fi
            if cache:
                try:
                    st = fi.path.stat()
                    cache.set(fi.path, st.st_mtime, st.st_size, method, algo, chunk_size, digest)
                except Exception:
                    pass
    return out

# ------------------ subtitle detection ------------------
def find_subtitles_for(src: Path) -> List[str]:
    """Return list of subtitle file paths (absolute strings) near the source file."""
    out = []
    parent = src.parent
    candidates_dirs = [parent]
    subs_dir = parent / "Subs"
    if subs_dir.is_dir():
        candidates_dirs.append(subs_dir)
    for d in candidates_dirs:
        try:
            for p in d.iterdir():
                if not p.is_file():
                    continue
                if p.suffix.lower() not in SUB_EXTS:
                    continue
                if src.stem.lower() in p.stem.lower() or p.stem.lower().startswith(src.stem.lower()):
                    out.append(str(p))
            # loosened-match fallback
            if not out:
                for p in d.iterdir():
                    if not p.is_file():
                        continue
                    if p.suffix.lower() not in SUB_EXTS:
                        continue
                    if len(set(src.stem.lower().split()) & set(p.stem.lower().split())) > 0:
                        out.append(str(p))
        except PermissionError:
            continue
    return out

# ------------------ cli ------------------
def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Compare video files (by extension + min size) and emit missing-file records.")
    ap.add_argument("--src", required=True, type=Path, help="Source root (e.g., /transmission/Downloads)")
    ap.add_argument("--dst", required=True, type=Path, help="Destination root (e.g., /plex/Media)")
    ap.add_argument("--format", choices=("text", "jsonl"), default="text", help="Output format. jsonl is pipeline-friendly.")
    ap.add_argument("--algo", default="blake2b", help="Hash algorithm")
    ap.add_argument("--method", choices=("full", "partial"), default="partial", help="Hash method")
    ap.add_argument("--chunk-size", type=int, default=4 * 1024 * 1024, help="Bytes for partial hashing")
    ap.add_argument("--min-size", default="100M", type=str, help="Ignore files smaller than this")
    ap.add_argument("--workers", default=os.cpu_count() or 4, type=int, help="Thread pool size")
    ap.add_argument("--cache", default=str(DEFAULT_CACHE), help="SQLite cache path or 'none'")
    ap.add_argument("--follow-symlinks", action="store_true")
    args = ap.parse_args(argv)

    min_size = parse_size(args.min_size)
    if not args.src.exists() or not args.src.is_dir() or not args.dst.exists() or not args.dst.is_dir():
        print("Source or destination path does not exist or is not a directory.", file=sys.stderr)
        return 2

    cache = None
    if args.cache and args.cache.lower() != "none":
        cache_path = Path(args.cache).expanduser()
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache = HashCache(cache_path)

    # scan: collect ALL video files by extension (>= min_size)
    src_files = collect_video_files(args.src, min_size, args.follow_symlinks)
    dst_files = collect_video_files(args.dst, min_size, args.follow_symlinks)

    # hash
    src_hashes = hash_map(src_files, args.algo, args.method, args.chunk_size, args.workers, cache)
    dst_hashes = hash_map(dst_files, args.algo, args.method, args.chunk_size, args.workers, cache)

    if cache:
        cache.close()

    dst_hash_set = set(dst_hashes.keys())
    missing = []
    for h, fi in src_hashes.items():
        if h not in dst_hash_set:
            missing.append(fi.path)

    if not missing:
        if args.format == "text":
            print("All matched.")
        return 0

    # for each missing file, build a record
    for p in missing:
        rel_dir = str(p.parent.relative_to(args.src))
        rec = {
            "src": str(p),
            "basename": p.name,
            "name": p.stem,
            "rel_dir": rel_dir,
            "size": p.stat().st_size,
            "ext": p.suffix,
            "missing_dir": str(Path(args.dst) / ".Missing" / rel_dir),
            "subtitles": find_subtitles_for(p),
        }

        if args.format == "jsonl":
            sys.stdout.write(json.dumps(rec, ensure_ascii=False) + "\n")
            sys.stdout.flush()
        else:
            print(f"- {rec['src']}")
            if rec["subtitles"]:
                print(f"    subtitles: {', '.join(rec['subtitles'])}")
    return 0

if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("Interrupted", file=sys.stderr)
        sys.exit(130)

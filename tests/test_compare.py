#!/usr/bin/env python3
import subprocess
import tempfile
import json
from pathlib import Path
import shutil
import os


# ------------------------------------------------------------------------------
# Locate compare script in repo root
# ------------------------------------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parents[1]
COMPARE = REPO_ROOT / "compare.py"
if not COMPARE.exists():
    raise RuntimeError(f"Could not find compare.py in {REPO_ROOT}")


def run_compare(src: Path, dst: Path, min_size="1M", follow_symlinks=False):
    """Run compare.py and return parsed JSONL lines as list of dicts."""
    cmd = [
        "python3",
        str(COMPARE),
        "--src",
        str(src),
        "--dst",
        str(dst),
        "--format",
        "jsonl",
        "--min-size",
        min_size,
    ]
    if follow_symlinks:
        cmd.append("--follow-symlinks")

    proc = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    # Fail loudly if the script itself failed (bad paths, etc.)
    assert proc.returncode == 0, (
        f"compare.py exited with {proc.returncode}\n"
        f"STDERR:\n{proc.stderr}"
    )

    lines = proc.stdout.strip().splitlines()
    out = []
    for line in lines:
        if line.strip():
            out.append(json.loads(line))
    return out


# ------------------------------------------------------------------------------
# Helper utilities
# ------------------------------------------------------------------------------

def make_file(path: Path, size_mb: int = 2):
    """Create a dummy file of a given size in MB."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as f:
        f.write(os.urandom(max(1, size_mb) * 1024 * 1024))


# ------------------------------------------------------------------------------
# TESTS 1–7, 9–11
# ------------------------------------------------------------------------------

def test_01_detect_missing_file():
    """1. Detect missing file (baseline)."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        src = root / "src"
        dst = root / "dst"
        src.mkdir()
        dst.mkdir()

        make_file(src / "MovieA" / "MovieA.mp4")  # ~2MB > default 1M
        # dst intentionally empty

        out = run_compare(src, dst)
        assert len(out) == 1
        assert out[0]["src"].endswith("MovieA/MovieA.mp4")


def test_02_detect_nothing_missing_when_files_match():
    """2. Detect nothing missing when files match."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        src = root / "src"
        dst = root / "dst"
        src.mkdir()
        dst.mkdir()

        f1 = src / "MovieA" / "MovieA.mp4"
        f2 = dst / "MovieA" / "MovieA.mp4"
        make_file(f1)
        f2.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(f1, f2)

        out = run_compare(src, dst)
        assert out == []  # empty JSONL list means “all matched”


def test_03_hash_cache_reuse():
    """3. Hash cache reuse (script runs fine with cache file)."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        cache = root / "cache.db"

        src = root / "src"
        dst = root / "dst"
        src.mkdir()
        dst.mkdir()

        f1 = src / "MovieX" / "MovieX.mp4"
        make_file(f1)

        # First run: populates cache (we only assert success)
        proc1 = subprocess.run(
            [
                "python3",
                str(COMPARE),
                "--src",
                str(src),
                "--dst",
                str(dst),
                "--cache",
                str(cache),
                "--format",
                "jsonl",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        assert proc1.returncode == 0, f"first run failed: {proc1.stderr}"

        # Second run: should read from cache (again, just assert success)
        proc2 = subprocess.run(
            [
                "python3",
                str(COMPARE),
                "--src",
                str(src),
                "--dst",
                str(dst),
                "--cache",
                str(cache),
                "--format",
                "jsonl",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        assert proc2.returncode == 0, f"second run failed: {proc2.stderr}"


def test_04_ignore_small_files():
    """4. Ignore small files below min-size."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        src = root / "src"
        dst = root / "dst"
        src.mkdir()
        dst.mkdir()

        tiny = src / "TinyMovie" / "Tiny.mp4"
        tiny.parent.mkdir(parents=True, exist_ok=True)
        with open(tiny, "wb") as f:
            f.write(os.urandom(100 * 1024))  # 100KB

        out = run_compare(src, dst, min_size="1M")
        assert out == []  # ignored due to size


def test_05_detect_subtitles_same_folder():
    """5. Detect subtitles in same folder."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        src = root / "src"
        dst = root / "dst"
        src.mkdir()
        dst.mkdir()

        movie = src / "MovieA" / "MovieA.mp4"
        sub = src / "MovieA" / "MovieA.eng.srt"
        make_file(movie)
        sub.write_text("dummy")

        out = run_compare(src, dst)
        assert len(out) == 1
        assert "subtitles" in out[0]
        assert any("MovieA.eng.srt" in s for s in out[0]["subtitles"])


def test_06_subtitles_in_subfolder():
    """6. Detect subtitles in Subs/ subfolder."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        src = root / "src"
        dst = root / "dst"
        src.mkdir()
        dst.mkdir()

        movie = src / "MovieA" / "MovieA.mp4"
        subs_dir = src / "MovieA" / "Subs"
        sub = subs_dir / "MovieA.srt"

        make_file(movie)
        subs_dir.mkdir(parents=True, exist_ok=True)
        sub.write_text("hi")

        out = run_compare(src, dst)
        assert len(out) == 1
        assert any("MovieA.srt" in s for s in out[0]["subtitles"])


def test_07_unicode_filenames():
    """7. Handle unicode filenames."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        src = root / "src"
        dst = root / "dst"
        src.mkdir()
        dst.mkdir()

        f = src / "Amélie (2001)" / "Amélie.mp4"
        make_file(f)

        out = run_compare(src, dst)
        assert len(out) == 1
        assert "Amélie" in out[0]["src"]


def test_09_nested_file_still_detected():
    """9. Nested file still shows up as missing."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        src = root / "src"
        dst = root / "dst"
        src.mkdir()
        dst.mkdir()

        f = src / "MovieA" / "Disc1" / "MovieA.mp4"
        make_file(f)

        out = run_compare(src, dst)
        assert len(out) == 1
        assert "Disc1/MovieA.mp4" in out[0]["src"]


def test_10_follow_symlinks():
    """10. --follow-symlinks includes files behind symlinks."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        src = root / "src"
        dst = root / "dst"
        src.mkdir()
        dst.mkdir()

        real = src / "RealMovie" / "RealMovie.mp4"
        symlink_target = src / "LinkMovie" / "RealMovie.mp4"

        make_file(real)

        symlink_target.parent.mkdir(parents=True, exist_ok=True)
        os.symlink(real, symlink_target)

        # Without follow_symlinks → only the real file considered
        out1 = run_compare(src, dst, min_size="1M")
        assert len(out1) == 1

        # With follow_symlinks → symlink also considered, but hashes dedupe,
        # so we still end up with 1 unique missing record.
        out2 = run_compare(src, dst, min_size="1M", follow_symlinks=True)
        assert len(out2) == 1


def test_11_hash_mismatch_detected_as_missing():
    """11. Same name, different content → still treated as missing."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        src = root / "src"
        dst = root / "dst"
        src.mkdir()
        dst.mkdir()

        f1 = src / "MovieA" / "MovieA.mp4"
        f2 = dst / "MovieA" / "MovieA.mp4"

        make_file(f1)
        make_file(f2)  # different random content

        out = run_compare(src, dst)
        assert len(out) == 1
        assert out[0]["src"].endswith("MovieA/MovieA.mp4")


import json
import os
import subprocess
import tempfile
from pathlib import Path

LINK = Path(__file__).resolve().parents[1] / "link.py"


def make_file(path: Path, size_bytes: int = 1024 * 1024):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as f:
        f.write(os.urandom(size_bytes))

def run_link_interactive(records, library_root: Path, answers):
    """
    records: list[dict] -> JSONL fed to stdin
    answers: list[str]  -> mapped to FILEBROKE_TEST_INPUT lines in order
    """
    jsonl = "".join(json.dumps(r) + "\n" for r in records)

    env = os.environ.copy()
    env["FILEBROKE_TEST_INPUT"] = "\n".join(answers)

    proc = subprocess.run(
        ["python3", str(LINK), "--library-root", str(library_root), "--interactive"],
        input=jsonl,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
    )

    out_lines = []
    for line in proc.stdout.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith("{") and line.endswith("}"):
            out_lines.append(json.loads(line))

    return proc, out_lines

def test_interactive_tv_manual_path():
    """
    Interactive TV manual path using public-domain series:
      Sherlock Holmes (1954), TMDB id = 6560
      Season 1, Episode 1
    """
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        src_root = root / "src"
        lib_root = root / "Library"
        src_root.mkdir()
        lib_root.mkdir()

        # link.py expects these exact dirs:
        (lib_root / "Movies").mkdir()
        (lib_root / "TV Shows").mkdir()

        # Fake messy download path
        src_ep = src_root / "Badly.Formatted.Sherlock" / "Sherlock.Holmes.1954.E01.mp4"
        make_file(src_ep)

        rec = {
            "src": str(src_ep),
            "basename": src_ep.name,
            "name": src_ep.stem,
            "rel_dir": str(src_ep.parent.relative_to(src_root)),
            "size": src_ep.stat().st_size,
            "ext": src_ep.suffix,
            "missing_dir": str(lib_root / ".Missing" / src_ep.parent.relative_to(src_root)),
            "subtitles": [],
        }

        # Answers in order of prompts:
        # 1) Choice (y/n/a/i/q/p):   -> y
        # 2) (m)ovie or (t)v:        -> t
        # 3) Series title:           -> Sherlock Holmes (1954)
        # 4) Season number:          -> 1
        # 5) Episode number:         -> 1
        # 6) Year (YYYY, optional):  -> 1954
        # 7) TMDB show id (optional)-> 6560
        # 8) Create link? (y/N):     -> y
        answers = [
            "y",
            "t",
            "Sherlock Holmes (1954)",
            "1",
            "1",
            "1954",
            "6560",
            "y",
        ]

        proc, out_lines = run_link_interactive([rec], lib_root, answers)

        # script may exit 0 (all good) or 1 (some skipped); anything >1 is error
        assert proc.returncode in (0, 1), proc.stderr

        # Instead of requiring JSON, assert on the actual library result
        tv_files = list((lib_root / "TV Shows").rglob("*.mp4"))
        assert len(tv_files) == 1
        dst_path = tv_files[0]

        # Basic sanity checks on path & naming (case-insensitive)
        dst_str = str(dst_path)
        lower = dst_str.lower()

        assert "tv shows" in lower
        assert "sherlock holmes" in lower
        # season/episode encoded in name
        assert "s01" in dst_path.name.lower()
        assert "e01" in dst_path.name.lower()

def test_interactive_movie_manual_path():
    """
    Interactive Movie manual path using public-domain film:
      Night of the Living Dead (1968), TMDB id = 10331
    """
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        src_root = root / "src"
        lib_root = root / "Library"
        src_root.mkdir()
        lib_root.mkdir()

        # link.py expects these exact dirs:
        (lib_root / "Movies").mkdir()
        (lib_root / "TV Shows").mkdir()

        # Fake messy download structure
        src_movie = src_root / "Bad.Directory.NotLD" / "Night.Of.The.Living.Dead.1968.x264.mp4"
        make_file(src_movie)

        rec = {
            "src": str(src_movie),
            "basename": src_movie.name,
            "name": src_movie.stem,
            "rel_dir": str(src_movie.parent.relative_to(src_root)),
            "size": src_movie.stat().st_size,
            "ext": src_movie.suffix,
            "missing_dir": str(lib_root / ".Missing" / src_movie.parent.relative_to(src_root)),
            "subtitles": [],
        }

        # Interactive answers:
        # 1) Choice y/n/a/i/q/p?     -> y   (process)
        # 2) Movie or TV?            -> m   (movie)
        # 3) Movie title:            -> Night of the Living Dead
        # 4) Year (YYYY):            -> 1968
        # 5) TMDB movie id:          -> 10331
        # 6) Create link? (y/N)      -> y
        answers = [
            "y",
            "m",
            "Night of the Living Dead",
            "1968",
            "10331",
            "y",
        ]

        proc, out_lines = run_link_interactive([rec], lib_root, answers)

        # script may exit 0 or 1, anything else means error
        assert proc.returncode in (0, 1), proc.stderr

        # Validate filesystem results
        movie_files = list((lib_root / "Movies").rglob("*.mp4"))
        assert len(movie_files) == 1
        dst = movie_files[0]
        dst_str = str(dst)

        # Basic path correctness checks
        assert "Movies" in dst_str
        s = dst_str.lower()
        assert "night of the living dead" in s
        assert "(1968)" in s
        assert "{tmdb-10331}" in s


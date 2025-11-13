# FileBroke
A lightweight FileBot companion that hardlinks missing media into your Plex library when FileBot won’t.

---

## Usage

python3 compare.py \
  --src /mnt/drive/media/Transmission/Downloads \
  --dst /mnt/drive/media/Library \
  --format jsonl \
| python3 link.py \
  --library-root /mnt/drive/media/Library \
  --interactive \
  --verbose
  
## Interactive Link Mode

`link.py` runs in interactive mode by default.  
Each unmatched file from `/mnt/drive/Transmission/Downloads` is shown one by one:

1. You confirm whether to process it.  
2. FileBot runs (strict → non-strict) to identify the title.  
3. If FileBot can’t match it, FileBroke switches to **manual mode**.  
   - You choose whether it’s a **movie** or **TV episode**.  
   - Then enter the relevant metadata (title, year, TMDB ID, and for TV: season/episode).  
4. FileBroke creates an atomic **hardlink** into `/mnt/drive/Library/Movies` or `/mnt/drive/Library/TV Shows`.

---

### Example: Movie

```
SRC: /mnt/drive/Transmission/Downloads/Badly.Formatted.Movie.Directory/Badly.Formatted.Movie.Filename.mp4
[y] work on this file [n] skip [a] work on ALL [i] ignore ALL [q] quit [p] print JSON
Choice (y/n/a/i/q/p): y
Running FileBot strict test...
FileBot couldn't find it. Manual mode.
Is this a (m)ovie or (t)v episode? [m/t]: m
Title: Well Formatted Movie Title
Year (YYYY): 2020
TMDB id (optional digits; Enter to skip): 123456
-> /mnt/drive/Library/Movies/Well Formatted Movie Title (2020) - {tmdb-123456}/Well Formatted Movie Title (2020).mp4
```
---

### Example: TV Episode

```
SRC: /mnt/drive/Transmission/Downloads/Badly.Formatted.Tv.Directory/Badly.Formatted.Tv.Episode.Name.01.mp4
[y] work on this file [n] skip [a] work on ALL [i] ignore ALL [q] quit [p] print JSON
Choice (y/n/a/i/q/p): y
Running FileBot strict test...
FileBot couldn't find it. Manual mode.
Is this a (m)ovie or (t)v episode? [m/t]: t
Series title: Badly Formatted TV Show
Year (YYYY): 2015
Season number: 1
Episode number: 1
Episode title (optional): The Pilot
TMDB id (optional digits; Enter to skip): 654321
-> /mnt/drive/Library/TV Shows/Badly Formatted TV Show (2015) - {tmdb-654321}/Season 01/Badly Formatted TV Show - S01E01 - The Pilot.mp4
```

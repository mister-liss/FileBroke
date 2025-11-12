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

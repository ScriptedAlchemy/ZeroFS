from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from pathlib import Path

SOURCE = Path("source")
MIB = 1024 * 1024
SIZE = 768 * MIB
WINDOW = 4 * MIB


def generate(index):
    name = f"ext4-drain-{index:02d}"
    digest = hashlib.sha256()
    with (SOURCE / name).open("xb") as output:
        for offset in range(0, SIZE, WINDOW):
            block = hashlib.shake_256(f"zerofs-ext4-sustained:{index}:{offset}".encode()).digest(WINDOW)
            output.write(block)
            digest.update(block)
    return name, digest.hexdigest()


with ThreadPoolExecutor(max_workers=6) as pool:
    manifest = dict(pool.map(generate, range(6)))
(SOURCE / "sha256.json").write_text(json.dumps(manifest, sort_keys=True) + "\n")

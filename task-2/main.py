import os
import hashlib
from functools import reduce
import operator


def sha3_256_file(path, chunk_size=8192):
    h = hashlib.sha3_256()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def sorting_key_from_hash(hex_str):
    """Compute product of (digit + 1) for each hex digit in hex_str."""
    nums = [(int(c, 16) + 1) for c in hex_str]
    prod = reduce(operator.mul, nums, 1)
    return prod


def process_directory(dir_path, email):
    files = []
    for root, dirs, filenames in os.walk(dir_path):
        for fn in filenames:
            full = os.path.join(root, fn)
            files.append(full)
    files.sort()

    hashes = []
    for fpath in files:
        h = sha3_256_file(fpath)
        hashes.append(h)
    if len(hashes) != 256:
        raise ValueError(f"Expected 256 files, but got {len(hashes)}")

    hashes_sorted = sorted(hashes, key=sorting_key_from_hash)

    joined = "".join(hashes_sorted)

    final_string = joined + email.lower()

    h_final = hashlib.sha3_256(final_string.encode("utf-8")).hexdigest()
    return h_final, hashes_sorted


def main():
    dir_path = "./task2"
    email = "kalandarovj70@gmail.com"
    final_hash, sorted_hashes = process_directory(dir_path, email)

    print("Final SHA3-256:", final_hash)
    for h in sorted_hashes:
        print(h)


if __name__ == "__main__":
    main()

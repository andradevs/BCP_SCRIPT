import gzip
import os
import tempfile
import unittest
from pathlib import Path

from import_from_s3 import maybe_decompress_gzip as s3_decompress
from import_local import maybe_decompress_gzip as local_decompress


def _create_gz_file(tmpdir: Path, filename: str, payload: bytes) -> Path:
    source = tmpdir / filename
    gz_path = tmpdir / f"{filename}.gz"

    with open(source, "wb") as raw:
        raw.write(payload)

    with gzip.open(gz_path, "wb") as gz:
        gz.write(payload)

    return gz_path


class GzipStreamingTests(unittest.TestCase):
    def test_maybe_decompress_gzip_streams_and_preserves_content(self):
        payload = os.urandom(2 * 1024 * 1024)  # 2MB para simular carga alta

        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            gz_path = _create_gz_file(tmpdir, "sample.bcp", payload)

            # Funcoes devem descompactar para um arquivo sem extender .gz
            s3_target = s3_decompress(gz_path)
            local_target = local_decompress(gz_path)

            self.assertTrue(s3_target.exists())
            self.assertTrue(local_target.exists())
            self.assertEqual(s3_target.suffix, ".bcp")
            self.assertEqual(local_target.suffix, ".bcp")

            self.assertEqual(s3_target.read_bytes(), payload)
            self.assertEqual(local_target.read_bytes(), payload)

    def test_maybe_decompress_gzip_returns_original_for_non_gz(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "plain.bcp"
            path.write_text("conteudo simples")

            self.assertEqual(s3_decompress(path), path)
            self.assertEqual(local_decompress(path), path)

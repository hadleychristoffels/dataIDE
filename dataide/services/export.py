import csv
import io
import json
from datetime import datetime
from typing import Dict, List

import pandas as pd

from dataide.schemas import DataIDEPayload
from dataide.services.profile import compute_profile, profile_to_rows


def zip_tables(sample_rows: Dict[str, List[dict]]) -> bytes:
    mem = io.BytesIO()
    import zipfile

    with zipfile.ZipFile(mem, "w", zipfile.ZIP_DEFLATED) as zf:
        for table, rows in sample_rows.items():
            if not rows:
                continue
            cols = list(rows[0].keys())
            buf = io.StringIO()
            writer = csv.DictWriter(buf, fieldnames=cols)
            writer.writeheader()
            for r in rows:
                writer.writerow(r)
            zf.writestr(f"{table}.csv", buf.getvalue())
    mem.seek(0)
    return mem.read()


def build_export_zip(prompt: str, payload: DataIDEPayload) -> bytes:
    mem = io.BytesIO()
    import zipfile

    with zipfile.ZipFile(mem, "w", zipfile.ZIP_DEFLATED) as zf:
        for table, rows in payload.sample_rows.items():
            if not rows:
                continue
            cols = list(rows[0].keys())
            buf = io.StringIO()
            writer = csv.DictWriter(buf, fieldnames=cols)
            writer.writeheader()
            for r in rows:
                writer.writerow(r)
            zf.writestr(f"samples/{table}.csv", buf.getvalue())

        zf.writestr("payload.json", json.dumps(payload.model_dump(), indent=2))
        zf.writestr("erd.mmd", payload.mermaid_erd)

        prof = payload.profiling_summary or compute_profile(payload.sample_rows, payload.tables)
        zf.writestr("profiling.json", json.dumps(prof, indent=2))
        rows = profile_to_rows(prof)
        df = pd.DataFrame(rows, columns=["level", "table", "column", "metric", "value"])
        zf.writestr("profiling.csv", df.to_csv(index=False))

        readme = (
            "dataIDE export\n"
            f"Generated: {datetime.utcnow().isoformat()}Z\n"
            f"Prompt: {prompt}\n\n"
            "Files:\n"
            "- samples/*.csv — sample rows per table\n"
            "- payload.json — full structured payload\n"
            "- erd.mmd — Mermaid ERD\n"
            "- profiling.json — computed profiling summary\n"
            "- profiling.csv — flattened profiling table\n"
        )
        zf.writestr("README.txt", readme)

    mem.seek(0)
    return mem.read()



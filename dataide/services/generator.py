import json
import os
import random
import string
from typing import List

from dataide.schemas import ColumnMeta, DataIDEPayload, SuggestedChart, TableSchema
from dataide.services.profile import compute_profile
from dataide.services.charts import suggest_charts


def _rid(prefix: str = "C", n: int = 8) -> str:
    return prefix + "".join(random.choices(string.digits, k=n))


def synthetic_payload(user_prompt: str) -> DataIDEPayload:
    random.seed(42)

    customers = [
        {
            "customer_id": _rid("C"),
            "email": f"user{i}@example.com",
            "country": "GB",
            "signup_date": f"2024-0{(i%9)+1}-0{(i%27)+1}"
        }
        for i in range(20)
    ]
    orders = [
        {
            "order_id": _rid("O"),
            "customer_id": customers[i % 20]["customer_id"],
            "order_date": f"2025-0{(i%9)+1}-1{(i%9)}",
            "quantity": (i % 5) + 1,
            "amount_gbp": round(5 + random.random() * 95, 2)
        }
        for i in range(40)
    ]

    tables: List[TableSchema] = [
        TableSchema(
            name="customers",
            description="Registered customers",
            primary_key=["customer_id"],
            columns=[
                ColumnMeta(name="customer_id", dtype="string", description="Unique customer identifier", nullable=False),
                ColumnMeta(name="email", dtype="string", description="Contact email", pii="moderate"),
                ColumnMeta(name="country", dtype="string", description="ISO country code"),
                ColumnMeta(name="signup_date", dtype="date", semantics="ISO-8601 date"),
            ],
        ),
        TableSchema(
            name="orders",
            description="Customer orders in GBP",
            primary_key=["order_id"],
            foreign_keys=[{"column": "customer_id", "ref": "customers.customer_id"}],
            columns=[
                ColumnMeta(name="order_id", dtype="string", nullable=False),
                ColumnMeta(name="customer_id", dtype="string", nullable=False),
                ColumnMeta(name="order_date", dtype="date"),
                ColumnMeta(name="quantity", dtype="int"),
                ColumnMeta(name="amount_gbp", dtype="decimal", semantics="GBP currency"),
            ],
        ),
    ]

    mermaid = """erDiagram
    customers ||--o{ orders : places
    customers {
      string customer_id PK
      string email
      string country
      date signup_date
    }
    orders {
      string order_id PK
      string customer_id FK
      date order_date
      int quantity
      decimal amount_gbp
    }"""

    profiling = compute_profile({"customers": customers, "orders": orders}, tables)
    charts = suggest_charts({"customers": customers, "orders": orders})

    return DataIDEPayload(
        dataset_description=f"Synthetic dataset for: {user_prompt}. UK D2C example with customers and orders.",
        tables=tables,
        mermaid_erd=mermaid,
        sample_rows={"customers": customers, "orders": orders},
        profiling_summary=profiling,
        suggested_charts=charts,
        caveats=["Synthetic sample for plumbing. Set OPENAI_API_KEY to use GPT-5 for real generation."],
    )


def maybe_gpt5_payload(user_prompt: str) -> DataIDEPayload:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return synthetic_payload(user_prompt)

    try:
        from openai import OpenAI

        client = OpenAI(api_key=api_key)
        schema = DataIDEPayload.model_json_schema()
        system = (
            "You are dataIDE. Return ONLY JSON that validates against the provided JSON schema. "
            "Use Mermaid ERD syntax in 'mermaid_erd'. Provide realistic small samples (<= 50 rows/table). "
            "Prefer ISO-8601 dates; use GBP if UK context is implied."
        )

        resp = client.chat.completions.create(
            model="gpt-5",
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user_prompt},
            ],
            response_format={"type": "json_schema", "json_schema": schema},
        )
        data = json.loads(resp.choices[0].message.content)
        payload = DataIDEPayload.model_validate(data)
        payload.profiling_summary = compute_profile(payload.sample_rows, payload.tables)
        if not payload.suggested_charts:
            payload.suggested_charts = suggest_charts(payload.sample_rows)
        return payload
    except Exception:
        return synthetic_payload(user_prompt)



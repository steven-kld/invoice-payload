#!/usr/bin/env python3
"""
invoice_payload.py — Reads verified JSONs, groups/validates, POSTs to n8n endpoint.

Usage:
    python3 invoice_payload.py \
        --input-dir /tmp/invoices/ \
        --folder-id 1abc2def3ghi \
        --endpoint https://webhook.n8n.gdev.inc/webhook/ca443011-fe9d-43df-944e-6c69a058d654
"""

import argparse
import json
import os
import re
import sys
import urllib.request
from datetime import datetime


def sanitize(s):
    if not s:
        return s
    return re.sub(r"[^A-Za-z0-9 .+'\-()]", "", s)


def format_date(s):
    if not s:
        return s
    try:
        d = datetime.strptime(s, "%Y-%m-%d")
        return d.strftime("%d.%m.%Y")
    except ValueError:
        return s


def doc_prefix(doc_type):
    return {"credit_note": "credit note", "proforma": "proforma", "summary": "summary"}.get(doc_type, "invoice")


def extract_currencies(invoices):
    currencies = set()
    for inv in invoices or []:
        for t in inv.get("totals", []):
            if t.get("currency"):
                currencies.add(t["currency"])
    return sorted(currencies)


def clean_bank_account(acc):
    if not acc:
        return None
    return re.sub(r"\s+", "", str(acc))


def collect_bank_accounts(doc):
    accs = set()
    acc = doc.get("org_bank_acc")
    if acc:
        if isinstance(acc, list):
            accs.update(acc)
        else:
            accs.add(acc)
    for a in doc.get("additional_org_bank_accs", []):
        accs.add(a)
    return accs


def validate_bill_to(doc):
    errors = []
    name = (doc.get("bill_to_name") or "").lower()
    if "nexters" not in name or "global" not in name:
        errors.append(
            f"Ошибка в названии плательщика: указан '{doc.get('bill_to_name')}', ожидалось Nexters Global"
        )
        doc["bill_to_name"] = None
    return errors


def get_leading_type(docs):
    for d in docs:
        if d.get("document_type") == "proforma":
            return "proforma"
    for d in docs:
        if d.get("document_type") == "summary":
            return "summary"
    return "invoice"


def group_and_validate(docs):
    leading_type = get_leading_type(docs)
    errors = []
    critical_errors = []

    if leading_type == "proforma":
        proforma = next((d for d in docs if d.get("document_type") == "proforma"), None)
        others = [d for d in docs if d is not proforma]

        result = json.loads(json.dumps(proforma))
        pf_invoices = result.get("invoices", [])

        if len(pf_invoices) > 1:
            critical_errors.append("proforma_multiple_invoice_lines")

        for other in others:
            result.setdefault("invoices", []).extend(other.get("invoices", []))
            for a in collect_bank_accounts(other):
                if a not in collect_bank_accounts(result):
                    result.setdefault("additional_org_bank_accs", []).append(a)

        result["document_number"] = f"proforma {proforma.get('document_number', '')}"

    elif leading_type == "summary":
        summary = next((d for d in docs if d.get("document_type") == "summary"), None)
        others = [d for d in docs if d is not summary]

        result = json.loads(json.dumps(summary))
        summary_invoice_ids = {inv["invoice_no"] for inv in result.get("invoices", [])}
        extra_doc_numbers = []

        for other in others:
            for inv in other.get("invoices", []):
                if inv["invoice_no"] in summary_invoice_ids:
                    existing = next(
                        (si for si in result["invoices"] if si["invoice_no"] == inv["invoice_no"]),
                        None,
                    )
                    if existing:
                        if inv.get("totals"):
                            existing["totals"] = inv["totals"]
                        if inv.get("description"):
                            existing["description"] = inv["description"]
                else:
                    result["invoices"].append(inv)
                    extra_doc_numbers.append(
                        f"{doc_prefix(other.get('document_type', 'invoice'))} {inv['invoice_no']}"
                    )
            for a in collect_bank_accounts(other):
                if a not in collect_bank_accounts(result):
                    result.setdefault("additional_org_bank_accs", []).append(a)

        doc_num = f"summary {summary.get('document_number', '')}"
        if extra_doc_numbers:
            doc_num += ", " + ", ".join(extra_doc_numbers)
        result["document_number"] = doc_num

    else:
        result = json.loads(json.dumps(docs[0]))
        doc_num_parts = [
            f"{doc_prefix(result.get('document_type', 'invoice'))} {result.get('document_number', '')}"
        ]

        for doc in docs[1:]:
            if doc.get("bill_to_name") != result.get("bill_to_name"):
                critical_errors.append(
                    f"BILL TO MISMATCH: {result.get('bill_to_name')} vs {doc.get('bill_to_name')}"
                )
                result["bill_to_name"] = None

            if doc.get("issue_date") != result.get("issue_date"):
                if doc.get("issue_date") is None:
                    errors.append(f"issue_date is null in doc {doc.get('document_number')}")
                elif result.get("issue_date") is None:
                    result["issue_date"] = doc["issue_date"]
                else:
                    critical_errors.append(
                        f"ISSUE DATE MISMATCH: {result['issue_date']} vs {doc['issue_date']}"
                    )
                    result["issue_date"] = None

            result.setdefault("invoices", []).extend(doc.get("invoices", []))
            for a in collect_bank_accounts(doc):
                if a not in collect_bank_accounts(result):
                    result.setdefault("additional_org_bank_accs", []).append(a)

            doc_num_parts.append(
                f"{doc_prefix(doc.get('document_type', 'invoice'))} {doc.get('document_number', '')}"
            )

        result["document_number"] = ", ".join(doc_num_parts)

    # --- Post-processing ---
    bill_to_errors = validate_bill_to(result)
    errors.extend(bill_to_errors)

    all_accs = sorted(collect_bank_accounts(result))
    result["org_bank_acc"] = clean_bank_account(all_accs[0]) if all_accs else None
    result["additional_org_bank_accs"] = [clean_bank_account(a) for a in all_accs[1:]]

    result["currency"] = extract_currencies(result.get("invoices", []))

    result["invoices"] = [
        {
            "invoice_no": inv.get("invoice_no"),
            "description": inv.get("description"),
            "totals": [
                {
                    "currency": t.get("currency"),
                    "amount": float(t["amount"]) if t.get("amount") is not None else None,
                }
                for t in inv.get("totals", [])
            ],
        }
        for inv in result.get("invoices", [])
    ]

    if not result.get("issue_date"):
        result["issue_date"] = datetime.now().strftime("%Y-%m-%d")

    # Remove fields 1C doesn't need
    for key in ["verification_status", "verification_notes"]:
        result.pop(key, None)

    if errors:
        result["errors"] = errors
    if critical_errors:
        result["critical_errors"] = critical_errors

    return result


def build_payload(grouped, folder_id):
    return {
        "folder_id": folder_id,
        "org_name": grouped.get("org_name"),
        "document_type": grouped.get("document_type"),
        "document_number": grouped.get("document_number"),
        "org_bank_acc": grouped.get("org_bank_acc"),
        "additional_org_bank_accs": grouped.get("additional_org_bank_accs", []),
        "issue_date": grouped.get("issue_date"),
        "due_date": grouped.get("due_date"),
        "vat_percent": grouped.get("vat_percent"),
        "bill_to_name": grouped.get("bill_to_name"),
        "currency": grouped.get("currency", []),
        "invoices": grouped.get("invoices", []),
        "errors": grouped.get("errors", []),
        "critical_errors": grouped.get("critical_errors", []),
    }


def post_to_endpoint(payload, endpoint):
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        endpoint,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req) as resp:
            body = resp.read().decode("utf-8")
            print(f"POST {endpoint} → {resp.status}")
            if body:
                print(body[:500])
            return resp.status
    except urllib.error.HTTPError as e:
        print(f"POST failed: {e.code} {e.reason}")
        print(e.read().decode("utf-8")[:500])
        return e.code


def main():
    parser = argparse.ArgumentParser(description="Prepare and send 1C payload")
    parser.add_argument("--input-dir", required=True, help="Dir with *_verified.json files")
    parser.add_argument("--folder-id", required=True, help="Drive folder ID (for n8n to fetch originals)")
    parser.add_argument(
        "--endpoint",
        default="https://webhook.n8n.gdev.inc/webhook/ca443011-fe9d-43df-944e-6c69a058d654",
        help="n8n webhook URL",
    )
    args = parser.parse_args()

    # Read verified JSONs
    docs = []
    for fname in sorted(os.listdir(args.input_dir)):
        if fname.endswith("_verified.json"):
            with open(os.path.join(args.input_dir, fname), "r", encoding="utf-8") as f:
                docs.append(json.load(f))

    if not docs:
        print(json.dumps({"error": "No *_verified.json found in " + args.input_dir}))
        sys.exit(1)

    print(f"Found {len(docs)} verified document(s)")

    # Group and validate
    grouped = group_and_validate(docs)

    # Build payload
    payload = build_payload(grouped, args.folder_id)

    # Print payload
    print(json.dumps(payload, indent=2, ensure_ascii=False))

    # Send to n8n
    status = post_to_endpoint(payload, args.endpoint)
    sys.exit(0 if 200 <= status < 300 else 1)


if __name__ == "__main__":
    main()

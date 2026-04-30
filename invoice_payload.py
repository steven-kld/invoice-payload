#!/usr/bin/env python3
"""
invoice_payload.py — Groups verified JSONs, validates, outputs 1C-ready payload.
Does NOT send — routine uses curl for that (proxy-aware).

Usage:
    python3 invoice_payload.py \
        --input-dir /tmp/invoices/ \
        --folder-id 1abc2def3ghi \
        --jira-1c-id 000000038 \
        --output /tmp/1c_payload.json
"""

import argparse
import json
import os
import re
import sys
from copy import deepcopy
from datetime import datetime


# ═══════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════

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
    return {
        "credit_note": "credit note",
        "proforma": "proforma",
        "summary": "summary",
    }.get(doc_type, "invoice")


def generate_file_name(doc):
    doc_number = doc.get("document_number")
    issue_date = doc.get("issue_date")
    doc_type = doc.get("document_type", "invoice")
    original = doc.get("original_filename", "document.pdf")
    ext = os.path.splitext(original)[1] or ".pdf"

    if doc_number and issue_date:
        return f"{doc_prefix(doc_type)} {sanitize(doc_number)} dated {format_date(issue_date)}{ext}"
    return sanitize(os.path.splitext(original)[0]) + ext


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


def collect_bank_accounts(item):
    accs = set()
    acc = item.get("org_bank_acc")
    if acc:
        if isinstance(acc, list):
            accs.update(a for a in acc if a)
        elif acc:
            accs.add(acc)
    for a in item.get("additional_org_bank_accs", []):
        if a:
            accs.add(a)
    return accs


def validate_bill_to(obj):
    name = (obj.get("bill_to_name") or "").lower()
    if "nexters" not in name or "global" not in name:
        obj.setdefault("critical_errors", []).append(
            f"Ошибка в названии плательщика, указан {obj.get('bill_to_name') or 'null'}, ожидалось Nexters Global"
        )
        obj["bill_to_name"] = None


def get_leading_type(group):
    for item in group:
        if item.get("document_type") == "proforma":
            return "proforma"
    for item in group:
        if item.get("document_type") == "summary":
            return "summary"
    return "invoice"


def build_file_entry(doc):
    return {
        "id": doc.get("document_number"),
        "file_name": generate_file_name(doc),
        "drive_file_id": doc.get("drive_file_id"),
    }


def clean_invoices(invoices):
    return [
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
        for inv in (invoices or [])
    ]


# ═══════════════════════════════════════════════════════════
# GROUPING (port of n8n JS)
# ═══════════════════════════════════════════════════════════

def group_and_validate(docs):
    leading_type = get_leading_type(docs)
    errors = []
    critical_errors = []

    for doc in docs:
        if "files" not in doc:
            doc["files"] = [build_file_entry(doc)]

    if leading_type == "proforma":
        proforma = next((d for d in docs if d.get("document_type") == "proforma"), None)
        others = [d for d in docs if d is not proforma]

        pf_invoices = proforma.get("invoices", [])
        if len(pf_invoices) > 1:
            critical_errors.append("proforma_multiple_invoice_lines")

        if not proforma.get("totals"):
            if pf_invoices and pf_invoices[0].get("totals"):
                proforma["totals"] = pf_invoices[0]["totals"]
            else:
                critical_errors.append("proforma_without_totals")

        base = deepcopy(proforma)
        base["invoices"] = []

        for other in others:
            base["invoices"].extend(other.get("invoices", []))
            base.setdefault("files", []).extend(other.get("files", []))
            for a in collect_bank_accounts(other):
                if a not in collect_bank_accounts(base):
                    base.setdefault("additional_org_bank_accs", []).append(a)

        base["document_number"] = f"proforma {proforma.get('document_number', '')}"

        all_accs = sorted(collect_bank_accounts(base))
        base["org_bank_acc"] = list(all_accs)
        base.pop("additional_org_bank_accs", None)

        base["currency"] = extract_currencies(base.get("invoices", []))
        if base.get("totals"):
            for t in base["totals"]:
                if t.get("currency") and t["currency"] not in base["currency"]:
                    base["currency"].append(t["currency"])

        result = base

    elif leading_type == "summary":
        summary = next((d for d in docs if d.get("document_type") == "summary"), None)
        others = [d for d in docs if d is not summary]

        base = deepcopy(summary)
        if not base.get("org_bank_acc"):
            base["org_bank_acc"] = []

        summary_invoice_ids = {inv["invoice_no"] for inv in base.get("invoices", [])}
        extra_doc_numbers = []

        for other in others:
            base.setdefault("files", []).extend(other.get("files", []))

            for inv in other.get("invoices", []):
                if inv["invoice_no"] in summary_invoice_ids:
                    existing = next(
                        (si for si in base["invoices"] if si["invoice_no"] == inv["invoice_no"]),
                        None,
                    )
                    if existing:
                        if inv.get("totals"):
                            existing["totals"] = inv["totals"]
                        if inv.get("description"):
                            existing["description"] = inv["description"]
                else:
                    base["invoices"].append(inv)
                    extra_doc_numbers.append(
                        f"{doc_prefix(other.get('document_type', 'invoice'))} {inv['invoice_no']}"
                    )

            for a in collect_bank_accounts(other):
                if a not in collect_bank_accounts(base):
                    if isinstance(base.get("org_bank_acc"), list):
                        base["org_bank_acc"].append(a)
                    else:
                        base.setdefault("additional_org_bank_accs", []).append(a)

        doc_num = f"summary {summary.get('document_number', '')}"
        if extra_doc_numbers:
            doc_num += ", " + ", ".join(extra_doc_numbers)
        base["document_number"] = doc_num

        base.pop("totals", None)

        all_accs = sorted(collect_bank_accounts(base))
        base["org_bank_acc"] = list(all_accs)
        base.pop("additional_org_bank_accs", None)

        base["currency"] = extract_currencies(base.get("invoices", []))

        result = base

    else:
        base = deepcopy(docs[0])
        bank_acc_set = collect_bank_accounts(base)

        doc_num_parts = [
            f"{doc_prefix(base.get('document_type', 'invoice'))} {base.get('document_number', '')}"
        ]

        for cur in docs[1:]:
            if cur.get("bill_to_name") != base.get("bill_to_name"):
                critical_errors.append(
                    f"BILL TO MISMATCH: {base.get('bill_to_name')} vs {cur.get('bill_to_name')}"
                )
                base["bill_to_name"] = None

            if cur.get("issue_date") != base.get("issue_date"):
                if cur.get("issue_date") is None:
                    errors.append(f"issue_date is null in doc {cur.get('document_number')}, kept {base.get('issue_date')}")
                elif base.get("issue_date") is None:
                    errors.append(f"issue_date was null, now {cur.get('issue_date')}")
                    base["issue_date"] = cur["issue_date"]
                else:
                    critical_errors.append(f"ISSUE DATE MISMATCH: {base['issue_date']} vs {cur['issue_date']}")
                    base["issue_date"] = None

            if cur.get("due_date") != base.get("due_date"):
                if cur.get("due_date") is None:
                    errors.append(f"due_date is null in doc {cur.get('document_number')}, kept {base.get('due_date')}")
                elif base.get("due_date") is None:
                    errors.append(f"due_date was null, now {cur.get('due_date')}")
                    base["due_date"] = cur["due_date"]
                else:
                    critical_errors.append(f"DUE DATE MISMATCH: {base['due_date']} vs {cur['due_date']}")
                    base["due_date"] = None

            base_vat = base.get("vat_percent") or 0
            cur_vat = cur.get("vat_percent") or 0
            if base_vat != cur_vat:
                critical_errors.append(f"VAT MISMATCH: {base.get('vat_percent')} vs {cur.get('vat_percent')}")
                base["vat_percent"] = None

            if cur.get("org_name") and cur["org_name"] != base.get("org_name"):
                if cur["org_name"] not in (base.get("org_name") or ""):
                    base["org_name"] = (base.get("org_name") or "") + " OR " + cur["org_name"]

            bank_acc_set.update(collect_bank_accounts(cur))
            base.setdefault("invoices", []).extend(cur.get("invoices", []))
            base.setdefault("files", []).extend(cur.get("files", []))

            doc_num_parts.append(
                f"{doc_prefix(cur.get('document_type', 'invoice'))} {cur.get('document_number', '')}"
            )

        base["document_number"] = ", ".join(doc_num_parts)
        base.pop("totals", None)

        base["org_bank_acc"] = sorted(bank_acc_set)
        base.pop("additional_org_bank_accs", None)
        base["currency"] = extract_currencies(base.get("invoices", []))

        result = base

    # --- Common post-processing ---
    validate_bill_to(result)

    if errors:
        result["errors"] = result.get("errors", []) + errors
    if critical_errors:
        result["critical_errors"] = result.get("critical_errors", []) + critical_errors

    result["invoices"] = clean_invoices(result.get("invoices", []))

    if not result.get("issue_date"):
        result["issue_date"] = datetime.now().strftime("%Y-%m-%d")

    # Mark has_file on every invoice
    file_ids = {f.get("id") for f in result.get("files", []) if f.get("id")}
    for inv in result.get("invoices", []):
        inv["has_file"] = inv.get("invoice_no") in file_ids

    # Clean bank accounts
    org_bank_acc = result.get("org_bank_acc")
    if isinstance(org_bank_acc, list):
        result["org_bank_acc"] = [clean_bank_account(a) for a in org_bank_acc if a]
    elif org_bank_acc:
        result["org_bank_acc"] = clean_bank_account(org_bank_acc)

    # Remove internal fields
    for key in ["verification_status", "verification_notes", "drive_file_id",
                "original_filename", "additional_org_bank_accs"]:
        result.pop(key, None)

    return result


# ═══════════════════════════════════════════════════════════
# BUILD PAYLOAD
# ═══════════════════════════════════════════════════════════

def build_payload(result, folder_id, jira_1c_id):
    return {
        "folder_id": folder_id,
        "jira_1c_id": jira_1c_id,
        "org_name": result.get("org_name"),
        "document_type": result.get("document_type"),
        "document_number": result.get("document_number"),
        "org_bank_acc": result.get("org_bank_acc"),
        "issue_date": result.get("issue_date"),
        "due_date": result.get("due_date"),
        "vat_percent": result.get("vat_percent"),
        "bill_to_name": result.get("bill_to_name"),
        "currency": result.get("currency", []),
        "invoices": result.get("invoices", []),
        "files": result.get("files", []),
        "errors": result.get("errors", []),
        "critical_errors": result.get("critical_errors", []),
    }


# ═══════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Prepare 1C payload from verified JSONs")
    parser.add_argument("--input-dir", required=True, help="Dir with *_verified.json files")
    parser.add_argument("--folder-id", required=True, help="Drive folder ID")
    parser.add_argument("--jira-1c-id", required=True, help="1C identifier from Jira Epic")
    parser.add_argument("--output", default="/tmp/1c_payload.json", help="Output file path")
    args = parser.parse_args()

    docs = []
    for fname in sorted(os.listdir(args.input_dir)):
        if fname.endswith("_verified.json"):
            with open(os.path.join(args.input_dir, fname), "r", encoding="utf-8") as f:
                docs.append(json.load(f))

    if not docs:
        print(json.dumps({"error": "No *_verified.json found in " + args.input_dir}))
        sys.exit(1)

    print(f"Found {len(docs)} verified document(s)")

    grouped = group_and_validate(docs)
    payload = build_payload(grouped, args.folder_id, args.jira_1c_id)

    # Save to file
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)

    # Print to stdout
    print(json.dumps(payload, indent=2, ensure_ascii=False))
    print(f"\nPayload saved to {args.output}")


if __name__ == "__main__":
    main()

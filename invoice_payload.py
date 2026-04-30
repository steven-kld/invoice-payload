#!/usr/bin/env python3
"""
prepare_1c_payload.py — Reads verified JSONs, groups/validates, outputs 1C-ready payload.

Usage:
    python3 prepare_1c_payload.py \
        --input-dir /tmp/invoices/ \
        --jira-epic-key PAY-123 \
        --jira-1c-id 12345678 \
        --folder-id 1abc2def3ghi \
        --email sender@company.com \
        --output /tmp/1c_payload.json
"""

import argparse
import json
import os
import re
import sys
from datetime import datetime


def sanitize(s):
    """Keep only safe chars for filenames."""
    if not s:
        return s
    return re.sub(r"[^A-Za-z0-9 .+'\-()]", "", s)


def format_date(s):
    """YYYY-MM-DD → DD.MM.YYYY"""
    if not s:
        return s
    try:
        d = datetime.strptime(s, "%Y-%m-%d")
        return d.strftime("%d.%m.%Y")
    except ValueError:
        return s


def doc_prefix(doc_type):
    prefixes = {
        "credit_note": "credit note",
        "proforma": "proforma",
        "summary": "summary",
    }
    return prefixes.get(doc_type, "invoice")


def generate_filename(doc, original_name="document.pdf"):
    """Generate 1C-friendly filename."""
    ext = os.path.splitext(original_name)[1] or ".pdf"
    doc_number = doc.get("document_number")
    issue_date = doc.get("issue_date")
    doc_type = doc.get("document_type", "invoice")

    if doc_number and issue_date:
        return f"{doc_prefix(doc_type)} {sanitize(doc_number)} dated {format_date(issue_date)}{ext}"
    
    return sanitize(os.path.splitext(original_name)[0]) + ext


def extract_currencies(invoices):
    """Collect unique currencies from invoices."""
    currencies = set()
    for inv in invoices or []:
        for t in inv.get("totals", []):
            if t.get("currency"):
                currencies.add(t["currency"])
    return sorted(currencies)


def clean_bank_account(acc):
    """Remove spaces from bank account."""
    if not acc:
        return None
    return re.sub(r"\s+", "", str(acc))


def validate_bill_to(doc):
    """Check that bill_to is Nexters Global."""
    errors = []
    name = (doc.get("bill_to_name") or "").lower()
    if "nexters" not in name or "global" not in name:
        errors.append(
            f"Ошибка в названии плательщика: указан '{doc.get('bill_to_name')}', "
            f"ожидалось Nexters Global"
        )
        doc["bill_to_name"] = None
    return errors


def get_leading_type(docs):
    """Determine leading document type for grouping."""
    for d in docs:
        if d.get("document_type") == "proforma":
            return "proforma"
    for d in docs:
        if d.get("document_type") == "summary":
            return "summary"
    return "invoice"


def collect_bank_accounts(doc):
    """Collect all bank accounts from a document."""
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


def group_and_validate(docs):
    """
    Group documents by type (proforma > summary > invoice).
    Merge invoices, validate fields, collect bank accounts.
    """
    leading_type = get_leading_type(docs)
    errors = []
    critical_errors = []

    if leading_type == "proforma":
        proforma = next((d for d in docs if d.get("document_type") == "proforma"), None)
        others = [d for d in docs if d is not proforma]

        result = json.loads(json.dumps(proforma))  # deep copy
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
                        None
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
        # Invoice / credit note
        result = json.loads(json.dumps(docs[0]))
        doc_num_parts = [f"{doc_prefix(result.get('document_type', 'invoice'))} {result.get('document_number', '')}"]

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

    # Common post-processing
    bill_to_errors = validate_bill_to(result)
    errors.extend(bill_to_errors)

    # Clean bank accounts
    all_accs = sorted(collect_bank_accounts(result))
    result["org_bank_acc"] = clean_bank_account(all_accs[0]) if all_accs else None
    result["additional_org_bank_accs"] = [clean_bank_account(a) for a in all_accs[1:]]

    # Currencies
    result["currency"] = extract_currencies(result.get("invoices", []))

    # Clean invoices
    result["invoices"] = [
        {
            "invoice_no": inv.get("invoice_no"),
            "description": inv.get("description"),
            "totals": [
                {
                    "currency": t.get("currency"),
                    "amount": float(t["amount"]) if t.get("amount") is not None else None
                }
                for t in inv.get("totals", [])
            ]
        }
        for inv in result.get("invoices", [])
    ]

    # Default issue_date
    if not result.get("issue_date"):
        result["issue_date"] = datetime.now().strftime("%Y-%m-%d")

    if errors:
        result["errors"] = errors
    if critical_errors:
        result["critical_errors"] = critical_errors

    return result


def build_payload(grouped, args):
    """Build final 1C payload."""
    return {
        "folder_id": args.folder_id,
        "jira_epic_key": args.jira_epic_key,
        "jira_1c_id": args.jira_1c_id,
        "email": args.email,
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


def main():
    parser = argparse.ArgumentParser(description="Prepare 1C payload from verified JSONs")
    parser.add_argument("--input-dir", required=True, help="Dir with verified JSON files")
    parser.add_argument("--jira-epic-key", default=None, help="Jira Epic key (PAY-123)")
    parser.add_argument("--jira-1c-id", default=None, help="1C identifier from Jira")
    parser.add_argument("--folder-id", required=True, help="Drive folder ID")
    parser.add_argument("--email", default=None, help="Sender email")
    parser.add_argument("--output", default="/tmp/1c_payload.json", help="Output path")
    args = parser.parse_args()

    # Read all verified JSONs
    docs = []
    input_dir = args.input_dir
    for fname in sorted(os.listdir(input_dir)):
        if fname.endswith("_verified.json"):
            path = os.path.join(input_dir, fname)
            with open(path, "r", encoding="utf-8") as f:
                docs.append(json.load(f))

    if not docs:
        print(json.dumps({"error": "No verified JSONs found in " + input_dir}))
        sys.exit(1)

    # Group and validate
    grouped = group_and_validate(docs)

    # Build payload
    payload = build_payload(grouped, args)

    # Write output
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    # Also print to stdout for routine to read
    print(json.dumps(payload, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()

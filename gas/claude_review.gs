/**
 * claude.gs — Claude API integration for invoice extraction & verification
 *
 * Script Properties used:
 *   ANTHROPIC_API_KEY
 */

/**
 * Extract structured JSON from Document AI entities + raw text.
 * @param {Array} entities - Document AI entities
 * @param {string} rawText - OCR raw text
 * @param {object} props - ScriptProperties
 * @returns {object|null} - parsed invoice JSON or null
 */
function callClaudeExtract(entities, rawText, props) {
  var apiKey = props.getProperty("ANTHROPIC_API_KEY");

  var entitiesSummary = entities.map(function(e) {
    var propsStr = "";
    if (e.properties && e.properties.length) {
      propsStr = " | properties: " + e.properties.map(function(p) {
        return p.type + "=" + (p.mentionText || "").substring(0, 80);
      }).join(", ");
    }
    return e.type + ": \"" + (e.mentionText || "").substring(0, 200) +
      "\" (confidence: " + (e.confidence || 0).toFixed(3) + ")" + propsStr;
  }).join("\n");

  var prompt = EXTRACTION_PROMPT
    .replace("{{ENTITIES}}", entitiesSummary)
    .replace("{{RAW_TEXT}}", rawText);

  var resp = callClaudeAPI_(apiKey, prompt, null);
  if (!resp) return null;

  try {
    return JSON.parse(resp.replace(/```json|```/g, "").trim());
  } catch (e) {
    Logger.log("Parse error (extract): " + e.message);
    Logger.log("Raw: " + resp.substring(0, 500));
    return null;
  }
}


/**
 * Verify extracted JSON against original PDF via vision.
 * @param {object} extracted - structured JSON from extraction step
 * @param {string} pdfBase64 - base64 encoded original document
 * @param {string} mimeType - document mime type
 * @param {object} props - ScriptProperties
 * @returns {object|null} - verified JSON or null
 */
function callClaudeVerify(extracted, pdfBase64, mimeType, props) {
  var apiKey = props.getProperty("ANTHROPIC_API_KEY");

  var prompt = VERIFICATION_PROMPT
    .replace("{{EXTRACTED_JSON}}", JSON.stringify(extracted, null, 2));

  var imageBlock = {
    type: "document",
    source: {
      type: "base64",
      media_type: mimeType || "application/pdf",
      data: pdfBase64
    }
  };

  var resp = callClaudeAPI_(apiKey, prompt, imageBlock);
  if (!resp) return null;

  try {
    return JSON.parse(resp.replace(/```json|```/g, "").trim());
  } catch (e) {
    Logger.log("Parse error (verify): " + e.message);
    Logger.log("Raw: " + resp.substring(0, 500));
    return null;
  }
}


// ═══════════════════════════════════════════════════════════
// LOW-LEVEL API CALL
// ═══════════════════════════════════════════════════════════

function callClaudeAPI_(apiKey, textPrompt, imageBlock) {
  var content = [];
  if (imageBlock) content.push(imageBlock);
  content.push({ type: "text", text: textPrompt });

  var payload = {
    model: "claude-sonnet-4-6",
    max_tokens: 4096,
    messages: [{ role: "user", content: content }]
  };

  var options = {
    method: "post",
    contentType: "application/json",
    headers: {
      "x-api-key": apiKey,
      "anthropic-version": "2023-06-01"
    },
    payload: JSON.stringify(payload),
    muteHttpExceptions: true
  };

  var resp = UrlFetchApp.fetch("https://api.anthropic.com/v1/messages", options);
  if (resp.getResponseCode() !== 200) {
    Logger.log("Claude API error (" + resp.getResponseCode() + "): " +
      resp.getContentText().substring(0, 500));
    return null;
  }

  var data = JSON.parse(resp.getContentText());
  var textBlocks = data.content.filter(function(b) { return b.type === "text"; });
  return textBlocks.length ? textBlocks[0].text : null;
}


// ═══════════════════════════════════════════════════════════
// PROMPTS
// ═══════════════════════════════════════════════════════════

var EXTRACTION_PROMPT =
  "OUTPUT JSON OF DEFINED STRUCTURE WITHOUT TEXT OR MARKDOWN\n\n" +
  "You are a financial document parser. Extract structured data from Document AI entities and raw text. Return a single valid JSON object.\n\n" +
  "STRUCTURE:\n" +
  "{\n" +
  "  \"org_name\": string,\n" +
  "  \"document_type\": \"proforma\" | \"summary\" | \"invoice\" | \"credit_note\",\n" +
  "  \"org_bank_acc\": string | null,\n" +
  "  \"additional_org_bank_accs\": [string],\n" +
  "  \"issue_date\": \"YYYY-MM-DD\",\n" +
  "  \"due_date\": \"YYYY-MM-DD\" | null,\n" +
  "  \"vat_percent\": float | null,\n" +
  "  \"document_number\": string,\n" +
  "  \"bill_to_name\": string,\n" +
  "  \"invoices\": [\n" +
  "    {\n" +
  "      \"invoice_no\": string,\n" +
  "      \"description\": string | null,\n" +
  "      \"totals\": [{ \"currency\": \"USD\", \"amount\": 1234.56 }]\n" +
  "    }\n" +
  "  ]\n" +
  "}\n\n" +
  "DOCUMENT TYPE:\n" +
  "- \"proforma\" — explicitly labeled as proforma/advance payment request\n" +
  "- \"summary\" — consolidated statement referencing multiple invoice IDs\n" +
  "- \"invoice\" — standard invoice for goods/services rendered\n" +
  "- \"credit_note\" — explicitly labeled as credit note/credit memo\n\n" +
  "BANK ACCOUNT:\n" +
  "- org_bank_acc: IBAN or account number ONLY. Remove spaces.\n" +
  "- additional_org_bank_accs: ONLY other bank account numbers or IBANs.\n" +
  "  Do NOT include: SWIFT/BIC, ABA routing, sort codes, BSB numbers.\n" +
  "  Purpose: counterparty lookup by account number.\n" +
  "- Extract from raw text if not in entities.\n\n" +
  "SUMMARY INVOICE (exclude from invoices array):\n" +
  "- Total equals sum of all others\n" +
  "- Number format differs from others\n" +
  "- Labeled \"Summary\" / \"Invoice Summary\"\n" +
  "- If only one invoice and looks like summary — include anyway\n\n" +
  "RULES:\n" +
  "- Dates: YYYY-MM-DD\n" +
  "- Amounts: numbers, remove thousand separators\n" +
  "- Missing optional field: null, never omit key\n" +
  "- vat_percent 0% → 0.0; not mentioned → null\n" +
  "- invoices: always array\n\n" +
  "DOCUMENT AI ENTITIES:\n{{ENTITIES}}\n\n" +
  "RAW DOCUMENT TEXT:\n{{RAW_TEXT}}";


var VERIFICATION_PROMPT =
  "You are verifying extracted invoice data against the original document.\n\n" +
  "EXTRACTED JSON:\n{{EXTRACTED_JSON}}\n\n" +
  "Compare every field with the original document image:\n" +
  "- org_name, document_number, issue_date, due_date\n" +
  "- amounts, currency, bank account, bill_to_name\n" +
  "- document_type correctness\n\n" +
  "Return the corrected JSON with added fields:\n" +
  "  \"verification_status\": \"verified\" | \"corrected\" | \"needs_review\"\n" +
  "  \"verification_notes\": string | null\n\n" +
  "OUTPUT ONLY VALID JSON, NO TEXT OR MARKDOWN.";


/**
 * Test full pipeline using real test folder in Drive
 * Contains: Unity invoice PDF + Document AI parsed.json
 */
function testFullPipelineFromFolder() {
  var props = PropertiesService.getScriptProperties();
  var folderId = "1euGTfBg0uKHxgoTeNiS26f873p8uwco0";
  var folder = DriveApp.getFolderById(folderId);
  var files = folder.getFiles();

  var pdfFile = null;
  var parsedFile = null;

  while (files.hasNext()) {
    var f = files.next();
    var name = f.getName();
    if (name.match(/_parsed\.json$/)) parsedFile = f;
    else if (name.match(/\.pdf$/i)) pdfFile = f;
  }

  if (!pdfFile || !parsedFile) {
    Logger.log("Missing files. PDF: " + !!pdfFile + ", Parsed: " + !!parsedFile);
    return;
  }

  Logger.log("PDF: " + pdfFile.getName());
  Logger.log("Parsed: " + parsedFile.getName());

  // Read parsed.json
  var parsedJson = JSON.parse(parsedFile.getBlob().getDataAsString());
  var doc = parsedJson.documentAI[0].document;
  var entities = doc.entities || [];
  var rawText = doc.text || "";

  Logger.log("Entities: " + entities.length + ", Text: " + rawText.length + " chars");

  // Step 1: Claude extract
  var extracted = callClaudeExtract(entities, rawText, props);
  if (!extracted) {
    Logger.log("EXTRACTION FAILED");
    return;
  }
  Logger.log("=== EXTRACTED ===");
  Logger.log(JSON.stringify(extracted, null, 2));

  // Step 2: Claude verify against original PDF
  var pdfBase64 = Utilities.base64Encode(pdfFile.getBlob().getBytes());
  var verified = callClaudeVerify(extracted, pdfBase64, pdfFile.getMimeType(), props);
  if (!verified) {
    Logger.log("VERIFICATION FAILED — using extracted");
    verified = extracted;
    verified.verification_status = "unverified";
  }

  Logger.log("=== VERIFIED ===");
  Logger.log(JSON.stringify(verified, null, 2));
  Logger.log("Status: " + verified.verification_status);

  // Step 3: Save result to same folder
  var resultName = pdfFile.getName().replace(/\.pdf$/i, "") + "_verified.json";
  folder.createFile(resultName, JSON.stringify(verified, null, 2), "application/json");
  Logger.log("Saved: " + resultName);
}

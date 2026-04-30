/**
 * main.gs — Email scanner + sequential document processor
 *
 * Script Properties:
 *   TRIGGER_ID         — routine trigger ID
 *   ROUTINE_TOKEN      — Bearer token for routine API
 *   DRIVE_FOLDER_ID    — root folder for invoice processing
 *   ANTHROPIC_API_KEY  — for Claude API calls
 */

function checkInvoiceEmails() {
  var props = PropertiesService.getScriptProperties();
  var threads = GmailApp.search("is:unread", 0, 5);

  if (!threads.length) {
    Logger.log("No new emails");
    return;
  }

  for (var t = 0; t < threads.length; t++) {
    var messages = threads[t].getMessages();
    for (var m = 0; m < messages.length; m++) {
      var msg = messages[m];
      if (!msg.isUnread()) continue;

      var result = processEmail(msg, props);
      msg.markRead();

      if (result) {
        // Schedule processing in separate execution (own 6-min timeout)
        PropertiesService.getScriptProperties().setProperty("PENDING_BATCH", JSON.stringify(result));
        ScriptApp.newTrigger("processBatch")
          .timeBased()
          .after(1000)
          .create();
        Logger.log("Batch scheduled: " + result.docCount + " docs in " + result.folderId);
      }
    }
  }
}


/**
 * Save all attachments to Drive, return batch metadata
 */
function processEmail(msg, props) {
  var rootFolderId = props.getProperty("DRIVE_FOLDER_ID");
  var rootFolder = DriveApp.getFolderById(rootFolderId);

  var subName = Utilities.formatDate(msg.getDate(), "GMT", "yyyyMMdd_HHmmss") +
    "_" + msg.getId().substring(0, 8);
  var emailFolder = rootFolder.createFolder(subName);

  var attachments = msg.getAttachments();
  var docCount = 0;

  for (var i = 0; i < attachments.length; i++) {
    var att = attachments[i];
    if (att.getContentType().match(/^image\//)) continue;

    emailFolder.createFile(att.copyBlob());
    docCount++;
    Logger.log("Saved: " + att.getName());
  }

  if (docCount === 0) {
    Logger.log("No document attachments in: " + msg.getSubject());
    return null;
  }

  return {
    folderId: emailFolder.getId(),
    docCount: docCount,
    emailFrom: msg.getFrom(),
    emailSubject: msg.getSubject(),
    gmailMessageId: msg.getId()
  };
}


// ═══════════════════════════════════════════════════════════
// BATCH PROCESSOR — one execution, all documents sequential
// ═══════════════════════════════════════════════════════════

function processBatch() {
  // Cleanup trigger
  var triggers = ScriptApp.getProjectTriggers();
  for (var i = 0; i < triggers.length; i++) {
    if (triggers[i].getHandlerFunction() === "processBatch") {
      ScriptApp.deleteTrigger(triggers[i]);
    }
  }

  var props = PropertiesService.getScriptProperties();
  var batchJson = props.getProperty("PENDING_BATCH");
  if (!batchJson) {
    Logger.log("No pending batch");
    return;
  }
  props.deleteProperty("PENDING_BATCH");

  var batch = JSON.parse(batchJson);
  var folder = DriveApp.getFolderById(batch.folderId);
  var files = folder.getFiles();

  var docs = [];
  while (files.hasNext()) {
    var f = files.next();
    if (f.getName().match(/\.(pdf|xlsx|xls|doc|docx)$/i)) {
      docs.push(f);
    }
  }

  Logger.log("Processing " + docs.length + " documents from: " + batch.emailSubject);

  var startTime = Date.now();
  var processed = 0;
  var errors = 0;

  for (var i = 0; i < docs.length; i++) {
    // Time check — leave 30 sec buffer
    if (Date.now() - startTime > 5.5 * 60 * 1000) {
      Logger.log("TIMEOUT: processed " + (processed + errors) + "/" + docs.length + ", scheduling continuation");

      var remaining = [];
      for (var j = i; j < docs.length; j++) {
        remaining.push(docs[j].getName());
      }

      var timeoutText = "ERROR: Invoice processing timed out.\n" +
        "From: " + batch.emailFrom + "\n" +
        "Subject: " + batch.emailSubject + "\n" +
        "Gmail message ID: " + batch.gmailMessageId + "\n" +
        "Drive folder ID: " + batch.folderId + "\n" +
        "Documents: " + batch.docCount + " total, " +
        processed + " verified, " + errors + " errors\n" +
        "Not processed: " + remaining.join(", ") + "\n" +
        "Timeout after " + Math.round((Date.now() - startTime) / 1000) + " seconds.";

      fireRoutine(timeoutText, props);
      return;
    }

    var file = docs[i];
    var name = file.getName();
    Logger.log("--- [" + (i + 1) + "/" + docs.length + "] " + name + " ---");

    try {
      var blob = file.getBlob();
      var base64 = Utilities.base64Encode(blob.getBytes());

      // Step 1: Document AI
      var docAiResult = callDocumentAI(base64, file.getMimeType());
      if (!docAiResult) {
        saveErrorJson(folder, name, "Document AI failed");
        errors++;
        continue;
      }
      Logger.log("Doc AI: " + docAiResult.entities.length + " entities");

      // Step 2: Claude extract
      var extracted = callClaudeExtract(docAiResult.entities, docAiResult.text, props);
      if (!extracted) {
        saveErrorJson(folder, name, "Claude extraction failed");
        errors++;
        continue;
      }
      Logger.log("Extracted: " + extracted.document_type + " " + (extracted.document_number || "no number"));

      // Step 3: Claude verify against original
      var verified = callClaudeVerify(extracted, base64, file.getMimeType(), props);
      var result = verified || extracted;

      if (!result.verification_status) {
        result.verification_status = verified ? "verified" : "unverified";
      }

      // Add Drive file reference for n8n to download and rename
      result.drive_file_id = file.getId();
      result.original_filename = name;

      // Save verified.json
      var jsonName = name.replace(/\.[^.]+$/, "") + "_verified.json";
      folder.createFile(jsonName, JSON.stringify(result, null, 2), "application/json");
      Logger.log("Saved: " + jsonName + " [" + result.verification_status + "]");
      processed++;

    } catch (e) {
      Logger.log("ERROR: " + name + " — " + e.message);
      saveErrorJson(folder, name, e.message);
      errors++;
    }
  }

  // All done — fire routine
  var elapsed = Math.round((Date.now() - startTime) / 1000);
  Logger.log("=== DONE: " + processed + " verified, " + errors + " errors, " + elapsed + "s ===");

  var text = "Invoice email fully processed.\n" +
    "From: " + batch.emailFrom + "\n" +
    "Subject: " + batch.emailSubject + "\n" +
    "Gmail message ID: " + batch.gmailMessageId + "\n" +
    "Drive folder ID: " + batch.folderId + "\n" +
    "Documents: " + batch.docCount + " total, " +
    processed + " verified, " + errors + " errors\n" +
    "Processing time: " + elapsed + " seconds.";

  fireRoutine(text, props);
  Logger.log("Routine fired");
}


// ═══════════════════════════════════════════════════════════
// HELPERS
// ═══════════════════════════════════════════════════════════

function saveErrorJson(folder, fileName, message) {
  var errorJson = {
    error: true,
    fileName: fileName,
    message: message,
    timestamp: new Date().toISOString()
  };
  var errorName = fileName.replace(/\.[^.]+$/, "") + "_error.json";
  folder.createFile(errorName, JSON.stringify(errorJson, null, 2), "application/json");
  Logger.log("Error saved: " + errorName);
}

function fireRoutine(text, props) {
  var triggerId = props.getProperty("TRIGGER_ID");
  var token = props.getProperty("ROUTINE_TOKEN");

  if (!triggerId || !token) {
    Logger.log("ROUTINE NOT CONFIGURED — would fire with:\n" + text);
    return;
  }

  var url = "https://api.anthropic.com/v1/claude_code/routines/" + triggerId + "/fire";

  var options = {
    method: "post",
    contentType: "application/json",
    headers: {
      "Authorization": "Bearer " + token,
      "anthropic-beta": "experimental-cc-routine-2026-04-01",
      "anthropic-version": "2023-06-01"
    },
    payload: JSON.stringify({ text: text }),
    muteHttpExceptions: true
  };

  var resp = UrlFetchApp.fetch(url, options);
  Logger.log("Routine: " + resp.getResponseCode());
}

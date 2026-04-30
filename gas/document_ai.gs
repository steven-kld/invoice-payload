/**
 * documentai.gs — Document AI integration via Service Account
 *
 * Script Properties used:
 *   SERVICE_ACCOUNT_KEY — JSON key file content (full JSON string)
 *
 * GCP Project: claude-agent-test-494910
 * Processor: TBD (create Invoice Parser in this project)
 */

var DOCAI_LOCATION = "us";
var DOCAI_PROJECT_ID = PropertiesService.getScriptProperties().getProperty("DOCAI_PROJECT_ID");
var DOCAI_PROCESSOR_ID = PropertiesService.getScriptProperties().getProperty("DOCAI_PROCESSOR_ID");

/**
 * Send document to Document AI for parsing.
 * @param {string} base64Content - base64 encoded file content
 * @param {string} mimeType - e.g. "application/pdf"
 * @returns {object|null} - {entities: [...], text: "..."} or null on error
 */
function callDocumentAI(base64Content, mimeType) {
  var props = PropertiesService.getScriptProperties();
  var token = getServiceAccountToken_(props);

  var url = "https://" + DOCAI_LOCATION + "-documentai.googleapis.com/v1/projects/" +
    DOCAI_PROJECT_ID + "/locations/" + DOCAI_LOCATION +
    "/processors/" + DOCAI_PROCESSOR_ID + ":process";

  var options = {
    method: "post",
    contentType: "application/json",
    headers: { "Authorization": "Bearer " + token },
    payload: JSON.stringify({
      rawDocument: { content: base64Content, mimeType: mimeType }
    }),
    muteHttpExceptions: true
  };

  var resp = UrlFetchApp.fetch(url, options);
  if (resp.getResponseCode() !== 200) {
    Logger.log("Document AI error (" + resp.getResponseCode() + "): " +
      resp.getContentText().substring(0, 500));
    return null;
  }

  var doc = (JSON.parse(resp.getContentText()).document) || {};
  return {
    entities: doc.entities || [],
    text: doc.text || ""
  };
}


// ═══════════════════════════════════════════════════════════
// SERVICE ACCOUNT AUTH (JWT → Access Token)
// ═══════════════════════════════════════════════════════════

/**
 * Get access token for service account via JWT grant.
 * Caches token in Script Properties for 50 min (tokens live 60 min).
 */
function getServiceAccountToken_(props) {
  // Check cache
  var cached = props.getProperty("SA_TOKEN_CACHE");
  if (cached) {
    var cache = JSON.parse(cached);
    if (cache.expires > Date.now()) {
      return cache.token;
    }
  }

  var keyJson = props.getProperty("SERVICE_ACCOUNT_KEY");
  if (!keyJson) throw new Error("SERVICE_ACCOUNT_KEY not set in Script Properties");

  var key = JSON.parse(keyJson);

  var url = "https://www.googleapis.com/oauth2/v4/token";
  var header = { alg: "RS256", typ: "JWT" };
  var now = Math.floor(Date.now() / 1000);
  var claim = {
    iss: key.client_email,
    scope: "https://www.googleapis.com/auth/cloud-platform",
    aud: url,
    exp: (now + 3600).toString(),
    iat: now.toString()
  };

  var signatureInput = Utilities.base64Encode(JSON.stringify(header)) + "." +
    Utilities.base64Encode(JSON.stringify(claim));
  var signature = Utilities.computeRsaSha256Signature(signatureInput, key.private_key);
  var jwt = signatureInput + "." + Utilities.base64Encode(signature);

  var resp = UrlFetchApp.fetch(url, {
    method: "post",
    payload: {
      grant_type: "urn:ietf:params:oauth:grant-type:jwt-bearer",
      assertion: jwt
    },
    muteHttpExceptions: true
  });

  if (resp.getResponseCode() !== 200) {
    throw new Error("SA token error (" + resp.getResponseCode() + "): " +
      resp.getContentText().substring(0, 300));
  }

  var token = JSON.parse(resp.getContentText()).access_token;

  // Cache for 50 min
  props.setProperty("SA_TOKEN_CACHE", JSON.stringify({
    token: token,
    expires: Date.now() + 50 * 60 * 1000
  }));

  return token;
}


/**
 * Base64url encode string without padding (RFC 7515)
 */
function b64url_(str) {
  return Utilities.base64EncodeWebSafe(
    Utilities.newBlob(str).getBytes()
  ).replace(/=+$/, "");
}


// ═══════════════════════════════════════════════════════════
// TEST
// ═══════════════════════════════════════════════════════════

/**
 * Test service account token retrieval
 */
function testServiceAccountAuth() {
  var props = PropertiesService.getScriptProperties();
  
  var keyJson = props.getProperty("SERVICE_ACCOUNT_KEY");
  if (!keyJson) {
    Logger.log("ERROR: SERVICE_ACCOUNT_KEY not set");
    return;
  }
  
  var key = JSON.parse(keyJson);
  Logger.log("Service account email: " + key.client_email);
  
  var url = "https://www.googleapis.com/oauth2/v4/token";
  var header = { alg: "RS256", typ: "JWT" };
  var now = Math.floor(Date.now() / 1000);
  var claim = {
    iss: key.client_email,
    scope: "https://www.googleapis.com/auth/cloud-platform",
    aud: url,
    exp: (now + 3600).toString(),
    iat: now.toString()
  };

  var signatureInput = Utilities.base64Encode(JSON.stringify(header)) + "." +
    Utilities.base64Encode(JSON.stringify(claim));
  var signature = Utilities.computeRsaSha256Signature(signatureInput, key.private_key);
  var jwt = signatureInput + "." + Utilities.base64Encode(signature);

  Logger.log("JWT length: " + jwt.length);

  var resp = UrlFetchApp.fetch(url, {
    method: "post",
    payload: {
      grant_type: "urn:ietf:params:oauth:grant-type:jwt-bearer",
      assertion: jwt
    },
    muteHttpExceptions: true
  });

  Logger.log("Response code: " + resp.getResponseCode());
  Logger.log("Response body: " + resp.getContentText().substring(0, 300));
}

/**
 * Test Document AI on a specific Drive file
 */
function testDocumentAI() {
  var testFolderId = "abcd";
  var fileId = "123_efjk";

  var file = DriveApp.getFileById(fileId);
  var blob = file.getBlob();
  var base64 = Utilities.base64Encode(blob.getBytes());
  var mimeType = file.getMimeType();

  Logger.log("File: " + file.getName());
  Logger.log("Base64 length: " + base64.length);

  var result = callDocumentAI(base64, mimeType);
  if (!result) {
    Logger.log("Document AI failed");
    return;
  }

  Logger.log("Entities: " + result.entities.length + ", Text: " + result.text.length + " chars");

  for (var i = 0; i < result.entities.length; i++) {
    var e = result.entities[i];
    Logger.log("  " + e.type + ": \"" +
      (e.mentionText || "").substring(0, 80) +
      "\" (conf: " + (e.confidence || 0).toFixed(3) + ")");
  }

  // Save to test folder
  var folder = DriveApp.getFolderById(testFolderId);
  var jsonName = file.getName().replace(/\.[^.]+$/, "") + "_parsed_test.json";
  folder.createFile(jsonName, JSON.stringify(result, null, 2), "application/json");
  Logger.log("Saved: " + jsonName);
}

import Buffer "mo:base/Buffer";
import Blob "mo:base/Blob";
import HashMap "mo:base/HashMap";
import Hash "mo:base/Hash";        // added
import Nat "mo:base/Nat";
import Nat32 "mo:base/Nat32";      // added
import Nat64 "mo:base/Nat64";
import Principal "mo:base/Principal";
import Text "mo:base/Text";
import Time "mo:base/Time";

persistent actor {
  // Types
  public type Item = {
    sku: Text;
    name: Text;
    quantity: Nat;
    price: Nat;
  };

  public type Status = { #Pending; #Paid; #Shipped; #Delivered; #Cancelled };

  public type Model = { #Order; #Payment };

  public type EntityLink = {
    model: Model;
    entityId: Text;
    txId: Nat;
    createdAt: Nat64;
  };

  public type Transaction = {
    id: Nat;
    buyer: Principal;
    items: [Item];
    total: Nat;
    currency: Text;
    status: Status;
    createdAt: Nat64;
    updatedAt: Nat64;
    note: ?Text;
  };

  public type Result<T, E> = { #ok: T; #err: E };

  public type PdfExport = {
    filename: Text;
    contentType: Text;
    bytes: Blob;
    orderCount: Nat;
    paymentCount: Nat;
    pageCount: Nat;
  };

  // Persistent state
  var nextId: Nat = 0;
  var stableTxs: [Transaction] = [];
  var stableLinks: [EntityLink] = [];

  // Transient state
  transient var txs = Buffer.Buffer<Transaction>(0);
  transient var links = Buffer.Buffer<EntityLink>(0);

  // Bespoke Nat hasher -> Hash.Hash (Nat32)
  func natHash(n: Nat): Hash.Hash {
    let mod64: Nat = 18446744073709551616; // 2^64
    let n64 = n % mod64;
    let mixed = (n64 * 11400714819323198485) % mod64; // Fibonacci hashing
    Nat32.fromNat(mixed % 4294967296)                 // fold to 32 bits
  };

  transient var byId = HashMap.HashMap<Nat, Nat>(32, Nat.equal, natHash); // id -> index

  // Upgrade hooks
  system func preupgrade() {
    stableTxs := Buffer.toArray(txs);
    stableLinks := Buffer.toArray(links);
  };

  system func postupgrade() {
    txs := Buffer.fromArray(stableTxs);
    stableTxs := [];
    links := Buffer.fromArray(stableLinks);
    stableLinks := [];
    byId := HashMap.HashMap<Nat, Nat>(txs.size() * 2 + 1, Nat.equal, natHash);
    var i: Nat = 0;
    while (i < txs.size()) {
      let t = txs.get(i);
      byId.put(t.id, i);
      i += 1;
    };
  };

  // Helpers
  func now(): Nat64 = Nat64.fromIntWrap(Time.now());

  func validateItems(items: [Item]): ?Text {
    if (items.size() == 0) return ?("No items in transaction");
    for (it in items.vals()) {
      if (it.quantity == 0) return ?("Item '" # it.sku # "' quantity must be > 0");
    };
    null
  };

  func computeTotal(items: [Item]): Nat {
    var sum: Nat = 0;
    for (it in items.vals()) { sum += it.price * it.quantity };
    sum
  };

  func min(a: Nat, b: Nat): Nat = if (a <= b) a else b;

  func modelEq(a: Model, b: Model): Bool {
    switch (a, b) {
      case (#Order, #Order) { true };
      case (#Payment, #Payment) { true };
      case _ { false };
    }
  };

  func statusToText(status: Status): Text {
    switch (status) {
      case (#Pending) { "Pending" };
      case (#Paid) { "Paid" };
      case (#Shipped) { "Shipped" };
      case (#Delivered) { "Delivered" };
      case (#Cancelled) { "Cancelled" };
    }
  };

  func leftPadZeros(n: Nat, width: Nat): Text {
    let t = Nat.toText(n);
    let len = Text.size(t);
    if (len >= width) return t;

    var out = "";
    var i: Nat = 0;
    while (i < (width - len)) {
      out #= "0";
      i += 1;
    };
    out # t
  };

  func takeText(value: Text, limit: Nat): Text {
    if (limit == 0) return "";
    var out = "";
    var i: Nat = 0;
    for (c in value.chars()) {
      if (i >= limit) { return out };
      out #= Text.fromChar(c);
      i += 1;
    };
    out
  };

  func truncateText(value: Text, limit: Nat): Text {
    if (Text.size(value) <= limit) return value;
    if (limit <= 3) return takeText(value, limit);
    takeText(value, limit - 3) # "..."
  };

  func escapePdfText(value: Text): Text {
    var out = "";
    for (c in value.chars()) {
      switch (c) {
        case ('\\') { out #= "\\\\" };
        case ('(') { out #= "\\(" };
        case (')') { out #= "\\)" };
        case ('\n') { out #= " " };
        case ('\r') { out #= " " };
        case _ { out #= Text.fromChar(c) };
      }
    };
    out
  };

  func buildReportLines(orderTxs: [Transaction], paymentTxs: [Transaction]): [Text] {
    let lines = Buffer.Buffer<Text>(0);

    var orderTotal: Nat = 0;
    for (tx in orderTxs.vals()) {
      orderTotal += tx.total;
    };

    var paymentTotal: Nat = 0;
    for (tx in paymentTxs.vals()) {
      paymentTotal += tx.total;
    };

    lines.add("BLOCKCHAIN TRANSACTION REPORT");
    lines.add("GeneratedAt(ns): " # Nat64.toText(now()));
    lines.add("Orders: " # Nat.toText(orderTxs.size()) # " | Payments: " # Nat.toText(paymentTxs.size()));
    lines.add("Order Amount Total: " # Nat.toText(orderTotal));
    lines.add("Payment Amount Total: " # Nat.toText(paymentTotal));
    lines.add("============================================================");
    lines.add(" ");

    lines.add("ORDER TRANSACTIONS");
    lines.add("------------------------------------------------------------");
    if (orderTxs.size() == 0) {
      lines.add("No order transactions found.");
    } else {
      for (tx in orderTxs.vals()) {
        lines.add("Order Tx ID: " # Nat.toText(tx.id));
        lines.add("Buyer: " # Principal.toText(tx.buyer));
        lines.add("Status: " # statusToText(tx.status) # " | Currency: " # tx.currency # " | Total: " # Nat.toText(tx.total));
        lines.add("CreatedAt(ns): " # Nat64.toText(tx.createdAt) # " | UpdatedAt(ns): " # Nat64.toText(tx.updatedAt));
        switch (tx.note) {
          case (?note) { lines.add("Note: " # truncateText(note, 180)) };
          case null { lines.add("Note: -") };
        };
        lines.add("Items:");
        if (tx.items.size() == 0) {
          lines.add("  - none");
        } else {
          for (item in tx.items.vals()) {
            let subtotal = item.price * item.quantity;
            lines.add(
              "  - SKU=" # item.sku #
              " | Name=" # truncateText(item.name, 70) #
              " | Qty=" # Nat.toText(item.quantity) #
              " | Price=" # Nat.toText(item.price) #
              " | Subtotal=" # Nat.toText(subtotal)
            );
          };
        };
        lines.add(" ");
      };
    };

    lines.add(" ");
    lines.add("PAYMENT TRANSACTIONS");
    lines.add("------------------------------------------------------------");
    if (paymentTxs.size() == 0) {
      lines.add("No payment transactions found.");
    } else {
      for (tx in paymentTxs.vals()) {
        lines.add("Payment Tx ID: " # Nat.toText(tx.id));
        lines.add("Buyer: " # Principal.toText(tx.buyer));
        lines.add("Status: " # statusToText(tx.status) # " | Currency: " # tx.currency # " | Total: " # Nat.toText(tx.total));
        lines.add("CreatedAt(ns): " # Nat64.toText(tx.createdAt) # " | UpdatedAt(ns): " # Nat64.toText(tx.updatedAt));
        switch (tx.note) {
          case (?note) { lines.add("Note: " # truncateText(note, 180)) };
          case null { lines.add("Note: -") };
        };
        lines.add("Items:");
        if (tx.items.size() == 0) {
          lines.add("  - none");
        } else {
          for (item in tx.items.vals()) {
            let subtotal = item.price * item.quantity;
            lines.add(
              "  - SKU=" # item.sku #
              " | Name=" # truncateText(item.name, 70) #
              " | Qty=" # Nat.toText(item.quantity) #
              " | Price=" # Nat.toText(item.price) #
              " | Subtotal=" # Nat.toText(subtotal)
            );
          };
        };
        lines.add(" ");
      };
    };

    Buffer.toArray(lines)
  };

  func buildPdfFromLines(lines: [Text]): (Blob, Nat) {
    let linesPerPage: Nat = 44;
    let totalLines = lines.size();
    let pageCount = if (totalLines == 0) 1 else (totalLines + linesPerPage - 1) / linesPerPage;

    let contentStreams = Buffer.Buffer<Text>(pageCount);
    var p: Nat = 0;
    while (p < pageCount) {
      let stream = Buffer.Buffer<Text>(0);
    // Page header with slightly larger title and smaller subtitle
    stream.add("BT\n/F1 16 Tf\n50 810 Td\n");
    stream.add("(" # escapePdfText("Blockchain Transaction Report") # ") Tj\n");
    stream.add("0 -20 Td\n");
    stream.add("/F1 10 Tf\n");
    stream.add("(" # escapePdfText("Page " # Nat.toText(p + 1) # " of " # Nat.toText(pageCount)) # ") Tj\n");
    stream.add("0 -18 Td\n");

      let start = p * linesPerPage;
      let end = min(totalLines, start + linesPerPage);
      var i = start;
      while (i < end) {
    let rawLine = lines[i];
    let line = truncateText(rawLine, 110);

    // Simple styling: larger font for section headers, smaller for dividers, normal for body.
    if (Text.startsWith(rawLine, #text "BLOCKCHAIN TRANSACTION REPORT") or
      Text.startsWith(rawLine, #text "ORDER TRANSACTIONS") or
      Text.startsWith(rawLine, #text "PAYMENT TRANSACTIONS")) {
      stream.add("/F1 12 Tf\n");
    } else if (Text.startsWith(rawLine, #text "===") or Text.startsWith(rawLine, #text "---")) {
      stream.add("/F1 9 Tf\n");
    } else {
      stream.add("/F1 10 Tf\n");
    };

    stream.add("(" # escapePdfText(line) # ") Tj\n");
        if (i + 1 < end) {
          stream.add("0 -16 Td\n");
        };
        i += 1;
      };

      stream.add("\nET\n");
      contentStreams.add(Text.join("", Buffer.toArray(stream).vals()));
      p += 1;
    };

    // Object IDs
    // 1: Catalog, 2: Pages, 3: Font
    // Then per page: page object then content object
    let objectCount = 3 + (pageCount * 2);
    let objectBodies = Buffer.Buffer<Text>(objectCount);
    let pageRefs = Buffer.Buffer<Text>(pageCount);

    // 1) Catalog
    objectBodies.add("<< /Type /Catalog /Pages 2 0 R >>");

    // 2) Pages (placeholder, filled after page refs are known)
    objectBodies.add("");

    // 3) Font
    objectBodies.add("<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>");

    var pageIndex: Nat = 0;
    while (pageIndex < pageCount) {
      let pageId = 4 + (pageIndex * 2);
      let contentId = pageId + 1;
      pageRefs.add(Nat.toText(pageId) # " 0 R");

      let pageBody =
        "<< /Type /Page /Parent 2 0 R /MediaBox [0 0 595 842] " #
        "/Resources << /Font << /F1 3 0 R >> >> " #
        "/Contents " # Nat.toText(contentId) # " 0 R >>";
      objectBodies.add(pageBody);

      let stream = contentStreams.get(pageIndex);
      let contentBody =
        "<< /Length " # Nat.toText(Text.size(stream)) # " >>\n" #
        "stream\n" #
        stream #
        "endstream";
      objectBodies.add(contentBody);

      pageIndex += 1;
    };

    // Fill Pages object now that page refs are ready
    objectBodies.put(
      1,
      "<< /Type /Pages /Kids [" # Text.join(" ", Buffer.toArray(pageRefs).vals()) # "] /Count " # Nat.toText(pageCount) # " >>"
    );

    let header = "%PDF-1.4\n";
    let chunks = Buffer.Buffer<Text>(objectCount + 6);
    chunks.add(header);

    let offsets = Buffer.Buffer<Nat>(objectCount + 1);
    offsets.add(0); // free object

    var running = Text.size(header);
    var objIndex: Nat = 0;
    while (objIndex < objectCount) {
      let objId = objIndex + 1;
      let body = objectBodies.get(objIndex);
      let objectText = Nat.toText(objId) # " 0 obj\n" # body # "\nendobj\n";
      offsets.add(running);
      chunks.add(objectText);
      running += Text.size(objectText);
      objIndex += 1;
    };

    let xrefOffset = running;
    chunks.add("xref\n");
    chunks.add("0 " # Nat.toText(objectCount + 1) # "\n");
    chunks.add("0000000000 65535 f \n");

    var offIndex: Nat = 1;
    while (offIndex < offsets.size()) {
      let off = offsets.get(offIndex);
      chunks.add(leftPadZeros(off, 10) # " 00000 n \n");
      offIndex += 1;
    };

    chunks.add(
      "trailer\n<< /Size " # Nat.toText(objectCount + 1) # " /Root 1 0 R >>\n" #
      "startxref\n" # Nat.toText(xrefOffset) # "\n%%EOF"
    );

    (Text.encodeUtf8(Text.join("", Buffer.toArray(chunks).vals())), pageCount)
  };

  func addEntityLink(model: Model, entityId: Text, txId: Nat) {
    if (Text.size(Text.trim(entityId, #text " \t\r\n")) == 0) return;
    links.add({
      model;
      entityId;
      txId;
      createdAt = now();
    });
  };

  func latestTxIdByEntity(model: Model, entityId: Text): ?Nat {
    var i: Nat = links.size();
    while (i > 0) {
      i -= 1;
      let link = links.get(i);
      if (modelEq(link.model, model) and link.entityId == entityId) {
        return ?link.txId;
      };
    };
    null
  };

  func listByEntity(model: Model, entityId: Text, offset: Nat, limit: Nat): [Transaction] {
    if (limit == 0) { return [] };
    let ids = Buffer.Buffer<Nat>(0);

    var i: Nat = 0;
    while (i < links.size()) {
      let link = links.get(i);
      if (modelEq(link.model, model) and link.entityId == entityId) {
        ids.add(link.txId);
      };
      i += 1;
    };

    let total = ids.size();
    if (offset >= total) { return [] };

    let end = min(total, offset + limit);
    let out = Buffer.Buffer<Transaction>(0);
    var j = offset;
    while (j < end) {
      let txId = ids.get(j);
      switch (byId.get(txId)) {
        case (?idx) { out.add(txs.get(idx)) };
        case null {};
      };
      j += 1;
    };

    Buffer.toArray(out)
  };

  func listAllByModel(model: Model): [Transaction] {
    let out = Buffer.Buffer<Transaction>(0);
    var i: Nat = 0;
    while (i < links.size()) {
      let link = links.get(i);
      if (modelEq(link.model, model)) {
        switch (byId.get(link.txId)) {
          case (?idx) { out.add(txs.get(idx)) };
          case null {};
        }
      };
      i += 1;
    };
    Buffer.toArray(out)
  };

  func recordTransactionInternal(caller: Principal, items: [Item], currency: Text, note: ?Text, status: Status): Result<Transaction, Text> {
    switch (validateItems(items)) {
      case (?msg) { #err msg };
      case null {
        let total = computeTotal(items);
        let id = nextId;
        nextId += 1;

        let t: Transaction = {
          id;
          buyer = caller;
          items;
          total;
          currency;
          status;
          createdAt = now();
          updatedAt = now();
          note;
        };

        let idx = txs.size();
        txs.add(t);
        byId.put(id, idx);
        #ok t
      };
    }
  };

  // API

  public shared ({ caller }) func recordOrderTransaction(orderId: Text, items: [Item], currency: Text, status: Status, note: ?Text): async Result<Transaction, Text> {
    if (Text.size(Text.trim(orderId, #text " \t\r\n")) == 0) {
      return #err "orderId is required";
    };
    let result = recordTransactionInternal(caller, items, currency, note, status);
    switch (result) {
      case (#ok tx) {
        addEntityLink(#Order, orderId, tx.id);
        #ok tx
      };
      case (#err msg) { #err msg };
    }
  };

  public shared ({ caller }) func recordPaymentTransaction(paymentId: Text, items: [Item], currency: Text, status: Status, note: ?Text): async Result<Transaction, Text> {
    if (Text.size(Text.trim(paymentId, #text " \t\r\n")) == 0) {
      return #err "paymentId is required";
    };
    let result = recordTransactionInternal(caller, items, currency, note, status);
    switch (result) {
      case (#ok tx) {
        addEntityLink(#Payment, paymentId, tx.id);
        #ok tx
      };
      case (#err msg) { #err msg };
    }
  };

  public query func count(): async Nat { txs.size() };

  public query func getOrderTransaction(orderId: Text): async ?Transaction {
    switch (latestTxIdByEntity(#Order, orderId)) {
      case (?txId) {
        switch (byId.get(txId)) {
          case (?i) { ?txs.get(i) };
          case null { null };
        }
      };
      case null { null };
    }
  };

  public query func getPaymentTransaction(paymentId: Text): async ?Transaction {
    switch (latestTxIdByEntity(#Payment, paymentId)) {
      case (?txId) {
        switch (byId.get(txId)) {
          case (?i) { ?txs.get(i) };
          case null { null };
        }
      };
      case null { null };
    }
  };

  public query func listOrderTransactions(orderId: Text, offset: Nat, limit: Nat): async [Transaction] {
    listByEntity(#Order, orderId, offset, limit)
  };

  public query func listPaymentTransactions(paymentId: Text, offset: Nat, limit: Nat): async [Transaction] {
    listByEntity(#Payment, paymentId, offset, limit)
  };

  public query func listAllOrderTransactions(): async [Transaction] {
    listAllByModel(#Order)
  };

  public query func listAllPaymentTransactions(): async [Transaction] {
    listAllByModel(#Payment)
  };

  public query func listTransactions(offset: Nat, limit: Nat): async [Transaction] {
    let n = txs.size();
    if (offset >= n or limit == 0) { return [] };
    let end = min(n, offset + limit);
    let out = Buffer.Buffer<Transaction>(0);
    var i = offset;
    while (i < end) {
      out.add(txs.get(i));
      i += 1;
    };
    Buffer.toArray(out)
  };

  public query func listTransactionsByBuyer(buyer: Principal, offset: Nat, limit: Nat): async [Transaction] {
    if (limit == 0) { return [] };
    let out = Buffer.Buffer<Transaction>(0);
    var skipped: Nat = 0;
    var taken: Nat = 0;
    var i: Nat = 0;
    let n = txs.size();
    while (i < n and taken < limit) {
      let t = txs.get(i);
      if (t.buyer == buyer) {
        if (skipped < offset) {
          skipped += 1;
        } else {
          out.add(t);
          taken += 1;
        };
      };
      i += 1;
    };
    Buffer.toArray(out)
  };

  // Exports all order and payment blockchain transactions to a readable PDF report.
  // The PDF bytes are returned directly from Motoko so callers can download/save it.
  public query func exportAllTransactionsPdf(): async PdfExport {
    let orderTxs = listAllByModel(#Order);
    let paymentTxs = listAllByModel(#Payment);
    let lines = buildReportLines(orderTxs, paymentTxs);
    let (pdfBytes, pages) = buildPdfFromLines(lines);

    {
      filename = "blockchain-transactions-report.pdf";
      contentType = "application/pdf";
      bytes = pdfBytes;
      orderCount = orderTxs.size();
      paymentCount = paymentTxs.size();
      pageCount = pages;
    }
  };

  public shared func updateStatus(id: Nat, status: Status): async Result<Transaction, Text> {
    switch (byId.get(id)) {
      case (?i) {
        let t = txs.get(i);
        let updated = { t with status = status; updatedAt = now() };
        txs.put(i, updated);
        #ok updated
      };
      case null { #err "Transaction not found" };
    }
  };

  public query func health(): async Text { "ok" };
}
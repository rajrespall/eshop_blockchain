import Buffer "mo:base/Buffer";
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
import Buffer "mo:base/Buffer";
import HashMap "mo:base/HashMap";
import Hash "mo:base/Hash";        // added
import Nat "mo:base/Nat";
import Nat32 "mo:base/Nat32";      // added
import Nat64 "mo:base/Nat64";
import Principal "mo:base/Principal";
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

  // Transient state
  transient var txs = Buffer.Buffer<Transaction>(0);

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
  };

  system func postupgrade() {
    txs := Buffer.fromArray(stableTxs);
    stableTxs := [];
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

  // API
  public shared ({ caller }) func recordTransaction(items: [Item], currency: Text, note: ?Text): async Result<Transaction, Text> {
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
          status = #Pending;
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

  public query func getTransaction(id: Nat): async ?Transaction {
    switch (byId.get(id)) {
      case (?i) { ?txs.get(i) };
      case null { null };
    }
  };

  public query func count(): async Nat { txs.size() };

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
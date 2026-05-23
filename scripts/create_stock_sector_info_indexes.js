// MongoDB indexes for stock_sector_info.
db = db.getSiblingDB("tradingagents");

db.stock_sector_info.createIndex(
    { full_symbol: 1, classify_system: 1 },
    { unique: true, name: "uk_full_symbol_classify_system" }
);

db.stock_sector_info.createIndex(
    { classify_system: 1, l1_code: 1 },
    { name: "idx_classify_l1" }
);

db.stock_sector_info.createIndex({ l2_code: 1 }, { name: "idx_l2_code" });
db.stock_sector_info.createIndex({ l3_code: 1 }, { name: "idx_l3_code" });
db.stock_sector_info.createIndex({ full_symbol: 1 }, { name: "idx_full_symbol" });

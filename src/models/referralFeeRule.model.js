const mongoose = require("mongoose");

const referralFeeRuleSchema = new mongoose.Schema(
  {
    // Category name this rule applies to (e.g., "Kitchen & Dining")
    category: { type: String, required: true },

    // Price range this rule applies to. Null means open-ended.
    priceMin: { type: Number, default: null },
    priceMax: { type: Number, default: null },

    // Apply target (e.g., 'item', 'variant', 'shipping', 'category'). Free-form for flexibility.
    applyTo: { type: String, default: "item" },

    // Referral fee percentage (e.g., 15 for 15%)
    feePercent: { type: Number, required: true },

    // Minimum fee in USD to charge if percentage yields less
    minFeeUSD: { type: Number, default: 0 },

    // Variant/Notes (e.g., "Apparel > Shoes", or region/version flag)
    variant: { type: String, default: "" },
  },
  {
    timestamps: true,
  }
);

// Helpful index for fast lookup by category/variant and price range
referralFeeRuleSchema.index({
  category: 1,
  variant: 1,
  priceMin: 1,
  priceMax: 1,
});

module.exports = mongoose.model("ReferralFeeRule", referralFeeRuleSchema);

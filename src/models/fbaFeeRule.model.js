const mongoose = require("mongoose");

const fbaFeeRuleSchema = new mongoose.Schema(
  {
    // Size tier, e.g., Small Standard, Large Standard, etc.
    tier: { type: String, required: true },

    // Weight range and unit
    weightMin: { type: Number, default: 0 },
    weightMax: { type: Number, default: null }, // null means open-ended
    unit: { type: String, default: "oz" }, // oz, lb, g, kg, etc.

    // Fixed FBA fulfillment fee in USD (simple case)
    feeUSD: { type: Number, default: null },

    // Tiered formula support
    baseUSD: { type: Number, default: null },
    overageRules: [
      {
        overThresholdValue: { type: Number, required: true },
        overThresholdUnit: { type: String, required: true },
        stepValue: { type: Number, required: true },
        stepUnit: { type: String, required: true },
        stepFeeUSD: { type: Number, required: true },
      },
    ],

    // Optional variant/notes (e.g., region or program)
    variant: { type: String, default: "" },
  },
  { timestamps: true }
);

fbaFeeRuleSchema.index({ tier: 1, variant: 1, weightMin: 1, weightMax: 1 });

module.exports = mongoose.model("FbaFeeRule", fbaFeeRuleSchema);

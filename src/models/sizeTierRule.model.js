const mongoose = require("mongoose");

const sizeTierRuleSchema = new mongoose.Schema(
  {
    // e.g., Small standard-size, Large standard-size, Small oversize, ...
    tier: { type: String, required: true },

    // Maximum shipping weight for this tier (null = open ended)
    shippingWeightMax: { type: Number, default: null },

    // Maximum dimensions for this tier (in unitLength)
    longestMax: { type: Number, default: null },
    medianMax: { type: Number, default: null },
    shortestMax: { type: Number, default: null },
    lengthGirthMax: { type: Number, default: null },

    // Units
    unitLength: { type: String, default: "in" }, // in, cm
    unitWeight: { type: String, default: "lb" }, // oz, lb, g, kg

    variant: { type: String, default: "" },
  },
  { timestamps: true }
);

sizeTierRuleSchema.index({ tier: 1, variant: 1 }, { unique: false });

module.exports = mongoose.model("SizeTierRule", sizeTierRuleSchema);

const mongoose = require("mongoose");

const filterTemplateSchema = new mongoose.Schema(
  {
    name: { type: String, required: true, unique: true },
    // Legacy filters field for backward compatibility
    filters: {
      bought: { type: String, default: "all" },
      newArrival: { type: String, default: "all" },
      fulfillment: { type: String, default: "all" },
      fbaFee: { type: String, default: "all" },
      price: { type: String, default: "all" },
      rating: { type: String, default: "all" },
    },
    // New complete state field
    state: {
      filters: {
        bought: { type: String, default: "all" },
        newArrival: { type: String, default: "all" },
        fulfillment: { type: String, default: "all" },
        fbaFee: { type: String, default: "all" },
        price: { type: String, default: "all" },
        referralFee: { type: String, default: "all" },
        rating: { type: String, default: "all" },
      },
      sellers: [{ type: String }],
      categories: [{ type: String }],
      sources: [{ type: String }],
      sort: { type: String, default: "" },
      displayMode: { type: String, default: "grid" },
      imageOnlyCols: { type: Number, default: 0 },
    },
    description: { type: String, default: "" },
    isDefault: { type: Boolean, default: false },
    createdBy: { type: String, default: "system" },
  },
  {
    timestamps: true,
  }
);

filterTemplateSchema.index({ name: 1 });
filterTemplateSchema.index({ isDefault: 1 });

module.exports = mongoose.model("FilterTemplate", filterTemplateSchema);

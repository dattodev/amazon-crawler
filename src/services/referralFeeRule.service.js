const ReferralFeeRule = require("../models/referralFeeRule.model");
const XLSX = require("xlsx");

const createRule = async (data) => {
  return await ReferralFeeRule.create({
    category: data.category,
    priceMin: data.priceMin ?? null,
    priceMax: data.priceMax ?? null,
    applyTo: data.applyTo || "item",
    feePercent: data.feePercent,
    minFeeUSD: data.minFeeUSD ?? 0,
    variant: data.variant || "",
  });
};

const getAllRules = async ({ category, variant } = {}) => {
  const query = {};
  if (category) query.category = category;
  if (variant) query.variant = variant;
  return await ReferralFeeRule.find(query).sort({ category: 1, priceMin: 1 });
};

const getRuleById = async (id) => {
  return await ReferralFeeRule.findById(id);
};

const updateRule = async (id, data) => {
  return await ReferralFeeRule.findByIdAndUpdate(
    id,
    {
      category: data.category,
      priceMin: data.priceMin ?? null,
      priceMax: data.priceMax ?? null,
      applyTo: data.applyTo || "item",
      feePercent: data.feePercent,
      minFeeUSD: data.minFeeUSD ?? 0,
      variant: data.variant || "",
    },
    { new: true }
  );
};

const deleteRule = async (id) => {
  return await ReferralFeeRule.findByIdAndDelete(id);
};

const deleteAllRules = async () => {
  return await ReferralFeeRule.deleteMany();
};

const importFromExcel = async (workbook) => {
  try {
    const sheetName = workbook.SheetNames[0];
    const sheet = workbook.Sheets[sheetName];
    const rows = XLSX.utils.sheet_to_json(sheet, { defval: "" });

    let created = 0;
    let updated = 0;
    let skipped = 0;
    for (const row of rows) {
      // Expected columns: Category, Price_Min, Price_Max, Apply_To, Fee_%, Min_Fee_USD, Variant
      const category = (row["Category"] || row.Category || "")
        .toString()
        .trim();
      if (!category) {
        skipped++;
        continue;
      }
      const priceMin =
        row["Price_Min"] !== "" ? Number(row["Price_Min"]) : null;
      const priceMaxVal =
        row["Price_Max"] !== "" ? Number(row["Price_Max"]) : 0;
      const priceMax = priceMaxVal === 0 ? null : priceMaxVal;
      const applyTo = (row["Apply_To"] || "total").toString().toLowerCase();
      const feePercent = row["Fee_%"] !== "" ? Number(row["Fee_%"]) : null;
      const minFeeUSD =
        row["Min_Fee_USD"] !== "" ? Number(row["Min_Fee_USD"]) : 0;
      const variant = (row["Variant"] || "").toString();

      if (feePercent === null) {
        skipped++;
        continue;
      }

      // Upsert by category + variant + price range
      const query = {
        category,
        variant,
        priceMin: priceMin ?? null,
        priceMax: priceMax ?? null,
      };
      const update = {
        $set: {
          category,
          variant,
          priceMin: priceMin ?? null,
          priceMax: priceMax ?? null,
          applyTo,
          feePercent,
          minFeeUSD,
        },
      };
      const options = {
        new: true,
        upsert: true,
        setDefaultsOnInsert: true,
        rawResult: true,
      };
      const res = await ReferralFeeRule.findOneAndUpdate(
        query,
        update,
        options
      );
      if (res && res.lastErrorObject && res.lastErrorObject.upserted) created++;
      else updated++;
    }

    return { created, updated, skipped, total: rows.length };
  } catch (error) {
    throw error;
  }
};

module.exports = {
  createRule,
  getAllRules,
  getRuleById,
  updateRule,
  deleteRule,
  deleteAllRules,
  importFromExcel,
};

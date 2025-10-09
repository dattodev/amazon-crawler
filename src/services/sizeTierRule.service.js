const SizeTierRule = require("../models/sizeTierRule.model");
const XLSX = require("xlsx");

const createRule = async (data) => {
  return await SizeTierRule.create({
    tier: data.tier,
    shippingWeightMax: data.shippingWeightMax ?? null,
    longestMax: data.longestMax ?? null,
    medianMax: data.medianMax ?? null,
    shortestMax: data.shortestMax ?? null,
    lengthGirthMax: data.lengthGirthMax ?? null,
    unitLength: data.unitLength || "in",
    unitWeight: data.unitWeight || "lb",
    variant: data.variant || "",
  });
};

const getAllRules = async ({ tier, variant } = {}) => {
  const q = {};
  if (tier) q.tier = tier;
  if (variant) q.variant = variant;

  // Sort by tier priority: standard -> oversize
  const tierOrder = {
    "Small standard-size": 1,
    "Large standard-size": 2,
    "Small oversize": 3,
    "Medium oversize": 4,
    "Large oversize": 5,
    "Special oversize": 6,
  };

  return await SizeTierRule.find(q)
    .sort({
      tier: 1,
      shippingWeightMax: 1,
      longestMax: 1,
    })
    .then((rules) => {
      // Custom sort by tier priority
      return rules.sort((a, b) => {
        const aOrder = tierOrder[a.tier] || 999;
        const bOrder = tierOrder[b.tier] || 999;
        return aOrder - bOrder;
      });
    });
};

const getRuleById = async (id) => SizeTierRule.findById(id);

const updateRule = async (id, data) => {
  return await SizeTierRule.findByIdAndUpdate(
    id,
    {
      tier: data.tier,
      shippingWeightMax: data.shippingWeightMax ?? null,
      longestMax: data.longestMax ?? null,
      medianMax: data.medianMax ?? null,
      shortestMax: data.shortestMax ?? null,
      lengthGirthMax: data.lengthGirthMax ?? null,
      unitLength: data.unitLength || "in",
      unitWeight: data.unitWeight || "lb",
      variant: data.variant || "",
    },
    { new: true }
  );
};

const deleteRule = async (id) => SizeTierRule.findByIdAndDelete(id);

const deleteAllRules = async () => SizeTierRule.deleteMany();

const importFromExcel = async (workbook) => {
  try {
    const sheet = workbook.Sheets[workbook.SheetNames[0]];
    const rows = XLSX.utils.sheet_to_json(sheet, { defval: "" });

    // Extract first numeric value from mixed text like "≤ 16 oz (1 lb)"
    const firstNumber = (val, def = null) => {
      if (val === undefined || val === null || val === "") return def;
      if (typeof val === "number") return val;
      const s = String(val);
      const m = s.match(/[\d.,]+/);
      if (!m) return def;
      const n = Number(m[0].replace(",", "."));
      return Number.isNaN(n) ? def : n;
    };

    // Normalize header: lowercase and strip non-alphanumerics so
    // "Length + girth" == "lengthgirth"
    const norm = (k) =>
      String(k || "")
        .toLowerCase()
        .replace(/[^a-z0-9]/g, "");
    const get = (row, name) => {
      const keys = Object.keys(row);
      const target = keys.find((k) => norm(k) === norm(name));
      return target ? row[target] : undefined;
    };

    let created = 0,
      updated = 0,
      skipped = 0;
    for (const row of rows) {
      const tier = (get(row, "Product size tier") || get(row, "tier") || "")
        .toString()
        .trim();
      if (!tier) {
        skipped++;
        continue;
      }
      const shipRaw =
        get(row, "Shipping weight1") ?? get(row, "Shipping weight");
      const shippingWeightMax = firstNumber(shipRaw);
      const longestMax = firstNumber(get(row, "Longest side"));
      const medianMax = firstNumber(get(row, "Median side"));
      const shortestMax = firstNumber(get(row, "Shortest side"));
      const lengthGirthMax = firstNumber(get(row, "Length + girth"));
      // Detect units from text; default to in/lb
      const unitLength = /cm/i.test(
        String(
          get(row, "Longest side") ||
            get(row, "Median side") ||
            get(row, "Shortest side") ||
            get(row, "Length + girth") ||
            ""
        )
      )
        ? "cm"
        : "in";
      const unitWeight = /oz/i.test(String(shipRaw || "")) ? "oz" : "lb";

      const query = { tier };
      const update = {
        tier,
        shippingWeightMax,
        longestMax,
        medianMax,
        shortestMax,
        lengthGirthMax,
        unitLength,
        unitWeight,
      };
      const res = await SizeTierRule.findOneAndUpdate(
        query,
        { $set: update },
        { new: true, upsert: true, rawResult: true }
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

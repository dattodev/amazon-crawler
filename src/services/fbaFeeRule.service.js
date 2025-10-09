const FbaFeeRule = require("../models/fbaFeeRule.model");

const createRule = async (data) => {
  return await FbaFeeRule.create({
    tier: data.tier,
    weightMin: data.weightMin ?? 0,
    weightMax: data.weightMax ?? null,
    unit: data.unit || "oz",
    feeUSD: data.feeUSD ?? null,
    baseUSD: data.baseUSD ?? null,
    overageRules: Array.isArray(data.overageRules) ? data.overageRules : [],
    variant: data.variant || "",
  });
};

const getAllRules = async ({ tier, variant } = {}) => {
  const query = {};
  if (tier) query.tier = tier;
  if (variant) query.variant = variant;

  // Sort by tier priority: standard -> oversize
  const tierOrder = {
    "Small Standard": 1,
    "Large Standard": 2,
    "Small Oversize": 3,
    "Medium Oversize": 4,
    "Large Oversize": 5,
    "Extra Large": 6,
  };

  return await FbaFeeRule.find(query)
    .sort({
      tier: 1,
      weightMin: 1,
    })
    .then((rules) => {
      // Custom sort by tier priority, then unit, then weightMin
      return rules.sort((a, b) => {
        const aOrder = tierOrder[a.tier] || 999;
        const bOrder = tierOrder[b.tier] || 999;
        if (aOrder !== bOrder) return aOrder - bOrder;

        // If same tier, sort by unit priority (oz first, then lb, then others)
        const unitOrder = { oz: 1, lb: 2 };
        const aUnitOrder = unitOrder[a.unit] || 999;
        const bUnitOrder = unitOrder[b.unit] || 999;
        if (aUnitOrder !== bUnitOrder) return aUnitOrder - bUnitOrder;

        // If same tier and unit, sort by weightMin
        return (a.weightMin || 0) - (b.weightMin || 0);
      });
    });
};

const getRuleById = async (id) => {
  return await FbaFeeRule.findById(id);
};

const updateRule = async (id, data) => {
  return await FbaFeeRule.findByIdAndUpdate(
    id,
    {
      tier: data.tier,
      weightMin: data.weightMin ?? 0,
      weightMax: data.weightMax ?? null,
      unit: data.unit || "oz",
      feeUSD: data.feeUSD ?? null,
      baseUSD: data.baseUSD ?? null,
      overageRules: Array.isArray(data.overageRules) ? data.overageRules : [],
      variant: data.variant || "",
    },
    { new: true }
  );
};

const deleteRule = async (id) => {
  return await FbaFeeRule.findByIdAndDelete(id);
};

const deleteAllRules = async () => {
  return await FbaFeeRule.deleteMany();
};

const importFromExcel = async (workbookOrPath) => {
  const XLSX = require("xlsx");
  try {
    // Support both: workbook object (memory) and legacy file path
    let workbook = workbookOrPath;
    if (!workbook || !workbook.SheetNames || !workbook.Sheets) {
      // Assume it's a path, keep backward compatibility for local/dev
      workbook = XLSX.readFile(String(workbookOrPath));
    }

    const sheetName = workbook.SheetNames[0];
    const sheet = workbook.Sheets[sheetName];
    const rows = XLSX.utils.sheet_to_json(sheet, { defval: "" });

    // Safe number parser: supports "1,25" -> 1.25, blanks -> default
    const toNumber = (val, def = null) => {
      if (val === undefined || val === null || val === "") return def;
      if (typeof val === "number") return val;
      const num = Number(String(val).trim().replace(",", "."));
      return isNaN(num) ? def : num;
    };

    // Parse patterns like "3 lb" or "4.00 oz" or "3,00 lb" → { value, unit }
    const parseValueUnit = (raw) => {
      if (raw === undefined || raw === null) return { value: null, unit: null };
      if (typeof raw === "number") return { value: raw, unit: null };
      const text = String(raw).trim();
      const m = text.match(/([\d.,]+)\s*([a-zA-Z]+)?/);
      if (!m) return { value: toNumber(text, null), unit: null };
      return { value: toNumber(m[1], null), unit: m[2] || null };
    };

    // Header tolerant getter: case-insensitive and ignores spaces/underscores
    const normalizeKey = (k) =>
      String(k || "")
        .toLowerCase()
        .replace(/\s+/g, "")
        .replace(/_/g, "");
    const get = (row, names) => {
      const nameList = Array.isArray(names) ? names : [names];
      const keys = Object.keys(row);
      for (const want of nameList) {
        const wantN = normalizeKey(want);
        const foundKey = keys.find((k) => normalizeKey(k) === wantN);
        if (foundKey !== undefined) return row[foundKey];
      }
      return undefined;
    };

    let created = 0,
      updated = 0,
      skipped = 0;
    for (const row of rows) {
      // Expected columns: Tier, Weight_Min, Weight_Max, Unit, Fee_USD, Base_USD, Overage_Threshold_Value, Overage_Threshold_Unit, Overage_Step_Value, Overage_Step_Unit, Overage_Step_Fee_USD, Variant
      const tier = (get(row, "Tier") || "").toString().trim();
      if (!tier) {
        skipped++;
        continue;
      }
      const weightMin = toNumber(get(row, "Weight_Min"), 0);
      const weightMaxVal = toNumber(get(row, "Weight_Max"), 0);
      const weightMax = weightMaxVal === 0 ? null : weightMaxVal;
      const unit = (get(row, "Unit") || "oz").toString();
      const feeUSD = toNumber(get(row, "Fee_USD"), null);
      const baseUSD = toNumber(get(row, "Base_USD"), null);
      // Support two formats:
      // 1) Separate columns: *_Value, *_Unit
      // 2) Combined in *_Value like "3 lb" (unit column empty)
      let { value: overThresholdValue, unit: overUnitFromValue } =
        parseValueUnit(get(row, "Overage_Threshold_Value"));
      let overThresholdUnit =
        get(row, "Overage_Threshold_Unit") || overUnitFromValue || null;
      let { value: stepValue, unit: stepUnitFromValue } = parseValueUnit(
        get(row, "Overage_Step_Value")
      );
      let stepUnit = get(row, "Overage_Step_Unit") || stepUnitFromValue || null;
      const stepFeeUSD = toNumber(get(row, "Overage_Step_Fee_USD"), null);
      const variant = (get(row, "Variant") || "").toString();

      if (feeUSD === null && baseUSD === null) {
        skipped++;
        continue;
      }

      const query = { tier, variant, weightMin, weightMax };
      const updateDoc = { tier, variant, weightMin, weightMax, unit };
      const hasTiered =
        baseUSD !== null ||
        overThresholdValue !== null ||
        overThresholdUnit ||
        stepValue !== null ||
        stepUnit ||
        stepFeeUSD !== null;
      if (hasTiered) {
        updateDoc.feeUSD = null;
        updateDoc.baseUSD = baseUSD;
        const thisOver =
          overThresholdValue !== null &&
          stepValue !== null &&
          stepFeeUSD !== null &&
          overThresholdUnit &&
          stepUnit
            ? [
                {
                  overThresholdValue,
                  overThresholdUnit,
                  stepValue,
                  stepUnit,
                  stepFeeUSD,
                },
              ]
            : [];
        // First: upsert base fields
        await FbaFeeRule.findOneAndUpdate(
          query,
          { $set: updateDoc },
          { new: true, upsert: true, setDefaultsOnInsert: true }
        );
        // Then: append overage rules (preventing duplicates)
        if (thisOver.length) {
          await FbaFeeRule.updateOne(query, {
            $addToSet: { overageRules: { $each: thisOver } },
          });
        }
        // skip normal update below since we've written already
        continue;
      } else {
        // Simple fixed fee when no tiered inputs provided
        updateDoc.feeUSD = feeUSD;
        updateDoc.baseUSD = null;
        updateDoc.overageRules = [];
      }
      const update = { $set: updateDoc };
      const options = {
        new: true,
        upsert: true,
        setDefaultsOnInsert: true,
        rawResult: true,
      };
      const res = await FbaFeeRule.findOneAndUpdate(query, update, options);
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

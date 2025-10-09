const ruleService = require("../services/referralFeeRule.service");

exports.createRule = async (req, res) => {
  try {
    const rule = await ruleService.createRule(req.body);
    res.status(201).json(rule);
  } catch (err) {
    res.status(400).json({ error: err.message });
  }
};

exports.getAllRules = async (req, res) => {
  try {
    const { category, variant } = req.query;
    const rules = await ruleService.getAllRules({ category, variant });
    res.json(rules);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
};

exports.getRuleById = async (req, res) => {
  try {
    const rule = await ruleService.getRuleById(req.params.id);
    if (!rule) return res.status(404).json({ error: "Rule not found" });
    res.json(rule);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
};

exports.updateRule = async (req, res) => {
  try {
    const rule = await ruleService.updateRule(req.params.id, req.body);
    if (!rule) return res.status(404).json({ error: "Rule not found" });
    res.json(rule);
  } catch (err) {
    res.status(400).json({ error: err.message });
  }
};

exports.deleteRule = async (req, res) => {
  try {
    const rule = await ruleService.deleteRule(req.params.id);
    if (!rule) return res.status(404).json({ error: "Rule not found" });
    res.json({ message: "Rule deleted" });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
};

exports.deleteAllRules = async (req, res) => {
  try {
    await ruleService.deleteAllRules();
    res.json({ message: "All rules deleted" });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
};

exports.importFromExcel = async (req, res) => {
  try {
    if (!req.excelWorkbook)
      return res.status(400).json({ error: "No Excel workbook found" });
    const result = await ruleService.importFromExcel(req.excelWorkbook);
    res.json(result);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
};

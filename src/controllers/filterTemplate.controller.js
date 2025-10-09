const filterTemplateService = require("../services/filterTemplate.service");

const createTemplate = async (req, res) => {
  try {
    const template = await filterTemplateService.createTemplate(req.body);
    res.status(201).json(template);
  } catch (error) {
    console.error("Error creating filter template:", error);
    res.status(400).json({ error: error.message });
  }
};

const getAllTemplates = async (req, res) => {
  try {
    const templates = await filterTemplateService.getAllTemplates();
    res.json(templates);
  } catch (error) {
    console.error("Error fetching filter templates:", error);
    res.status(500).json({ error: "Failed to fetch templates" });
  }
};

const getTemplateById = async (req, res) => {
  try {
    const template = await filterTemplateService.getTemplateById(req.params.id);
    if (!template) {
      return res.status(404).json({ error: "Template not found" });
    }
    res.json(template);
  } catch (error) {
    console.error("Error fetching filter template:", error);
    res.status(500).json({ error: "Failed to fetch template" });
  }
};

const updateTemplate = async (req, res) => {
  try {
    const template = await filterTemplateService.updateTemplate(
      req.params.id,
      req.body
    );
    if (!template) {
      return res.status(404).json({ error: "Template not found" });
    }
    res.json(template);
  } catch (error) {
    console.error("Error updating filter template:", error);
    res.status(400).json({ error: error.message });
  }
};

const deleteTemplate = async (req, res) => {
  try {
    const template = await filterTemplateService.deleteTemplate(req.params.id);
    if (!template) {
      return res.status(404).json({ error: "Template not found" });
    }
    res.json({ message: "Template deleted successfully" });
  } catch (error) {
    console.error("Error deleting filter template:", error);
    res.status(500).json({ error: "Failed to delete template" });
  }
};

const deleteAllTemplates = async (req, res) => {
  try {
    await filterTemplateService.deleteAllTemplates();
    res.json({ message: "All templates deleted successfully" });
  } catch (error) {
    console.error("Error deleting all templates:", error);
    res.status(500).json({ error: "Failed to delete templates" });
  }
};

const createDefaultTemplates = async (req, res) => {
  try {
    await filterTemplateService.createDefaultTemplates();
    res.json({ message: "Default templates created successfully" });
  } catch (error) {
    console.error("Error creating default templates:", error);
    res.status(500).json({ error: "Failed to create default templates" });
  }
};

module.exports = {
  createTemplate,
  getAllTemplates,
  getTemplateById,
  updateTemplate,
  deleteTemplate,
  deleteAllTemplates,
  createDefaultTemplates,
};

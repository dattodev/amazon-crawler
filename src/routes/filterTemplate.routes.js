const express = require("express");
const router = express.Router();
const filterTemplateController = require("../controllers/filterTemplate.controller");

// Create a new filter template
router.post("/", filterTemplateController.createTemplate);

// Get all filter templates
router.get("/", filterTemplateController.getAllTemplates);

// Get a specific filter template by ID
router.get("/:id", filterTemplateController.getTemplateById);

// Update a filter template
router.put("/:id", filterTemplateController.updateTemplate);

// Delete a filter template
router.delete("/:id", filterTemplateController.deleteTemplate);

// Delete all filter templates
router.delete("/", filterTemplateController.deleteAllTemplates);

// Create default templates
router.post(
  "/create-defaults",
  filterTemplateController.createDefaultTemplates
);

module.exports = router;

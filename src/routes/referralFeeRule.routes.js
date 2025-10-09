const express = require("express");
const router = express.Router();
const controller = require("../controllers/referralFeeRule.controller");
const {
  upload,
  processExcelFromBuffer,
} = require("../middleware/memoryUpload");

// CRUD operations
router.post("/fee-rules", controller.createRule);
router.get("/fee-rules", controller.getAllRules);
router.get("/fee-rules/:id", controller.getRuleById);
router.put("/fee-rules/:id", controller.updateRule);
router.delete("/fee-rules/:id", controller.deleteRule);

// Dangerous operations
router.post("/fee-rules/delete-all", controller.deleteAllRules);

// Import Excel
router.post(
  "/fee-rules/import",
  upload.single("file"),
  processExcelFromBuffer,
  controller.importFromExcel
);

module.exports = router;

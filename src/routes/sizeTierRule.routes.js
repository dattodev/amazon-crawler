const express = require("express");
const router = express.Router();
const {
  upload,
  processExcelFromBuffer,
} = require("../middleware/memoryUpload");
const ctrl = require("../controllers/sizeTierRule.controller");

router.get("/", ctrl.getAllRules);
router.get("/:id", ctrl.getRuleById);
router.post("/", express.json(), ctrl.createRule);
router.put("/:id", express.json(), ctrl.updateRule);
router.delete("/:id", ctrl.deleteRule);
router.delete("/", ctrl.deleteAllRules);
router.post(
  "/import",
  upload.single("file"),
  processExcelFromBuffer,
  ctrl.importFromExcel
);

module.exports = router;

const express = require('express');
const router = express.Router();
const productController = require('../controllers/product.controller');

// CRUD operations
router.post('/products', productController.createProduct);
router.get('/products', productController.getAllProducts);
router.get('/products/count', productController.getProductsCount);

// Utility endpoints - MUST be before /:id routes
router.get('/products/soldby-list', productController.getUniqueSoldBy);
router.get('/products/category-list', productController.getUniqueCategories);
router.get('/products/source-list', productController.getUniqueSources);

// Test endpoint to check database
router.get('/products/test', async (req, res) => {
	try {
		const productService = require('../services/product.service');
		const allProducts = await productService.getAllProducts();
		res.json({
			totalProducts: allProducts.length,
			sampleProduct: allProducts[0] || null,
			hasSourceField: allProducts.some((p) => p.source !== undefined),
		});
	} catch (error) {
		res.status(500).json({ error: error.message });
	}
});

// Dangerous operations - should be POST for safety
router.post('/products/delete-all', productController.deleteAllProducts);

// Parameterized routes - MUST be after specific routes
router.get('/products/:id', productController.getProductById);
router.put('/products/:id', productController.updateProduct);
router.delete('/products/:id', productController.deleteProduct);

module.exports = router;

const express = require('express');
const router = express.Router();
const {
	getAllSearchUrls,
	getSearchUrlById,
	createSearchUrl,
	updateSearchUrl,
	deleteSearchUrl,
	addProductToSearchUrl,
	getSearchUrlProducts,
	getAllProducts,
	updateLastCrawledAt,
	getSearchUrlStats,
	getProductInfoByUrl,
} = require('../controllers/searchUrl.controller');

// Middleware để validate ObjectId
const validateObjectId = (req, res, next) => {
	const { id } = req.params;
	if (!id.match(/^[0-9a-fA-F]{24}$/)) {
		return res.status(400).json({
			success: false,
			message: 'Invalid ID format',
		});
	}
	next();
};

// Routes cho search URLs - specific routes phải đặt trước dynamic routes
router.get('/', getAllSearchUrls); // GET /api/search-urls
router.get('/stats', getSearchUrlStats); // GET /api/search-urls/stats
router.get('/products', getAllProducts); // GET /api/search-urls/products
router.get('/product-info/:productUrl(*)', getProductInfoByUrl); // GET /api/search-urls/product-info/:productUrl
router.get('/:id', validateObjectId, getSearchUrlById); // GET /api/search-urls/:id
router.post('/', createSearchUrl); // POST /api/search-urls
router.put('/:id', validateObjectId, updateSearchUrl); // PUT /api/search-urls/:id
router.delete('/:id', validateObjectId, deleteSearchUrl); // DELETE /api/search-urls/:id

// Routes cho products trong search URL
router.get('/:id/products', validateObjectId, getSearchUrlProducts); // GET /api/search-urls/:id/products
router.post('/:id/products', validateObjectId, addProductToSearchUrl); // POST /api/search-urls/:id/products

// Routes cho cập nhật thời gian crawl
router.patch('/:id/last-crawled', validateObjectId, updateLastCrawledAt); // PATCH /api/search-urls/:id/last-crawled

module.exports = router;

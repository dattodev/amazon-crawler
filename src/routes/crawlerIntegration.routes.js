const express = require('express');
const router = express.Router();
const {
	handleMarketplaceCrawlResult,
	handleBestsellersCrawlResult,
	getUrlsToCrawl,
	updateCrawlTime,
	getCrawlStats,
} = require('../controllers/crawlerIntegration.controller');

// Routes cho crawler integration
router.post('/marketplace-result', handleMarketplaceCrawlResult); // POST /api/crawler/marketplace-result
router.post('/bestsellers-result', handleBestsellersCrawlResult); // POST /api/crawler/bestsellers-result
router.get('/urls-to-crawl', getUrlsToCrawl); // GET /api/crawler/urls-to-crawl
router.patch('/update-time', updateCrawlTime); // PATCH /api/crawler/update-time
router.get('/stats', getCrawlStats); // GET /api/crawler/stats

module.exports = router;

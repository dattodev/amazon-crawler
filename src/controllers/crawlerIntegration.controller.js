const CrawlerIntegration = require('../utils/crawlerIntegration');

/**
 * API endpoint để crawler gửi kết quả marketplace
 */
const handleMarketplaceCrawlResult = async (req, res) => {
	try {
		const { url, result } = req.body;

		if (!url || !result) {
			return res.status(400).json({
				success: false,
				message: 'URL and result are required',
			});
		}

		const integrationResult =
			await CrawlerIntegration.handleMarketplaceResult(url, result);

		res.json({
			success: true,
			message: 'Marketplace result processed successfully',
			data: integrationResult,
		});
	} catch (error) {
		console.error('Error processing marketplace result:', error);
		res.status(500).json({
			success: false,
			message: 'Error processing marketplace result',
			error: error.message,
		});
	}
};

/**
 * API endpoint để crawler gửi kết quả bestsellers
 */
const handleBestsellersCrawlResult = async (req, res) => {
	try {
		const { url, result } = req.body;

		if (!url || !result) {
			return res.status(400).json({
				success: false,
				message: 'URL and result are required',
			});
		}

		const integrationResult =
			await CrawlerIntegration.handleBestsellersResult(url, result);

		res.json({
			success: true,
			message: 'Bestsellers result processed successfully',
			data: integrationResult,
		});
	} catch (error) {
		console.error('Error processing bestsellers result:', error);
		res.status(500).json({
			success: false,
			message: 'Error processing bestsellers result',
			error: error.message,
		});
	}
};

/**
 * API endpoint để lấy danh sách URLs cần crawl
 */
const getUrlsToCrawl = async (req, res) => {
	try {
		const urls = await CrawlerIntegration.getUrlsToCrawl();

		res.json({
			success: true,
			data: urls,
		});
	} catch (error) {
		console.error('Error getting URLs to crawl:', error);
		res.status(500).json({
			success: false,
			message: 'Error getting URLs to crawl',
			error: error.message,
		});
	}
};

/**
 * API endpoint để cập nhật thời gian crawl
 */
const updateCrawlTime = async (req, res) => {
	try {
		const { url } = req.body;

		if (!url) {
			return res.status(400).json({
				success: false,
				message: 'URL is required',
			});
		}

		// Cập nhật thời gian crawl
		await CrawlerIntegration.updateCrawlTime(url);

		res.json({
			success: true,
			message: 'Crawl time updated successfully',
		});
	} catch (error) {
		console.error('Error updating crawl time:', error);
		res.status(500).json({
			success: false,
			message: 'Error updating crawl time',
			error: error.message,
		});
	}
};

/**
 * API endpoint để lấy thống kê crawl
 */
const getCrawlStats = async (req, res) => {
	try {
		const stats = await CrawlerIntegration.getCrawlStats();

		res.json({
			success: true,
			data: stats,
		});
	} catch (error) {
		console.error('Error getting crawl stats:', error);
		res.status(500).json({
			success: false,
			message: 'Error getting crawl stats',
			error: error.message,
		});
	}
};

module.exports = {
	handleMarketplaceCrawlResult,
	handleBestsellersCrawlResult,
	getUrlsToCrawl,
	updateCrawlTime,
	getCrawlStats,
};

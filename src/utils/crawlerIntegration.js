const SearchUrlService = require('../services/searchUrl.service');

/**
 * Tích hợp với crawler để lưu dữ liệu vào search_urls
 */
class CrawlerIntegration {
	/**
	 * Xử lý kết quả từ marketplace crawler
	 * @param {string} marketplaceUrl - URL của marketplace
	 * @param {Object} result - Kết quả từ crawler
	 */
	static async handleMarketplaceResult(marketplaceUrl, result) {
		try {
			console.log(`Processing marketplace result for: ${marketplaceUrl}`);

			// Tạo hoặc cập nhật search URL
			const searchUrl = await SearchUrlService.createOrUpdateSearchUrl(
				marketplaceUrl
			);

			if (result.success && result.products) {
				// Chuyển đổi dữ liệu sản phẩm
				const products = result.products.map((product) => ({
					name: product.name,
					asin: product.asin,
					url: product.url,
					source: product.source || 'marketplace',
					created_at: new Date(),
					updated_at: new Date(),
				}));

				// Thêm sản phẩm vào search URL
				const addResult = await SearchUrlService.addProductsToSearchUrl(
					marketplaceUrl,
					products
				);

				// Cập nhật thời gian crawl cuối cùng
				await SearchUrlService.updateLastCrawledAt(marketplaceUrl);

				console.log(
					`Marketplace crawl completed: ${addResult.added} new, ${addResult.updated} updated, ${addResult.total} total products`
				);

				return {
					success: true,
					searchUrlId: searchUrl._id,
					productsAdded: addResult.added,
					productsUpdated: addResult.updated,
					totalProducts: addResult.total,
				};
			} else {
				console.error(`Marketplace crawl failed: ${result.error}`);

				return {
					success: false,
					error: result.error,
				};
			}
		} catch (error) {
			console.error('Error handling marketplace result:', error);
			throw error;
		}
	}

	/**
	 * Xử lý kết quả từ bestsellers crawler
	 * @param {string} bestsellersUrl - URL của bestsellers
	 * @param {Object} result - Kết quả từ crawler
	 */
	static async handleBestsellersResult(bestsellersUrl, result) {
		try {
			console.log(`Processing bestsellers result for: ${bestsellersUrl}`);

			// Tạo hoặc cập nhật search URL
			const searchUrl = await SearchUrlService.createOrUpdateSearchUrl(
				bestsellersUrl
			);

			if (result.success && result.products) {
				// Chuyển đổi dữ liệu sản phẩm
				const products = result.products.map((product) => ({
					name: product.name || 'Unknown Product',
					asin: product.asin || this.extractAsinFromUrl(product.url),
					url: product.url,
					source: product.source || 'best-sellers',
					rank_b: product.rank_b || null, // Thêm rank_b field
					created_at: new Date(),
					updated_at: new Date(),
				}));

				// Thêm sản phẩm vào search URL
				const addResult = await SearchUrlService.addProductsToSearchUrl(
					bestsellersUrl,
					products
				);

				// Cập nhật thời gian crawl cuối cùng
				await SearchUrlService.updateLastCrawledAt(bestsellersUrl);

				console.log(
					`Bestsellers crawl completed: ${addResult.added} new, ${addResult.updated} updated, ${addResult.total} total products`
				);

				return {
					success: true,
					searchUrlId: searchUrl._id,
					productsAdded: addResult.added,
					productsUpdated: addResult.updated,
					totalProducts: addResult.total,
				};
			} else {
				console.error(`Bestsellers crawl failed: ${result.error}`);

				return {
					success: false,
					error: result.error,
				};
			}
		} catch (error) {
			console.error('Error handling bestsellers result:', error);
			throw error;
		}
	}

	/**
	 * Trích xuất ASIN từ URL
	 * @param {string} url - URL sản phẩm
	 * @returns {string} ASIN
	 */
	static extractAsinFromUrl(url) {
		if (!url) return null;

		const asinMatch = url.match(/\/dp\/([A-Z0-9]{10})/);
		return asinMatch ? asinMatch[1] : null;
	}

	/**
	 * Lấy danh sách URLs cần crawl
	 * @returns {Promise<Array>} Danh sách URLs
	 */
	static async getUrlsToCrawl() {
		try {
			const searchUrls = await SearchUrlService.getSearchUrlsToCrawl();
			return searchUrls.map((su) => ({
				id: su._id,
				url: su.url,
				config: su.config,
				lastCrawled: su.last_crawled_at,
			}));
		} catch (error) {
			console.error('Error getting URLs to crawl:', error);
			throw error;
		}
	}

	/**
	 * Cập nhật thời gian crawl
	 * @param {string} url - URL đã crawl
	 */
	static async updateCrawlTime(url) {
		try {
			await SearchUrlService.updateLastCrawlTime(url);
		} catch (error) {
			console.error('Error updating crawl time:', error);
			throw error;
		}
	}

	/**
	 * Lấy thống kê crawl
	 * @returns {Promise<Object>} Thống kê
	 */
	static async getCrawlStats() {
		try {
			return await SearchUrlService.getCrawlStats();
		} catch (error) {
			console.error('Error getting crawl stats:', error);
			throw error;
		}
	}
}

module.exports = CrawlerIntegration;

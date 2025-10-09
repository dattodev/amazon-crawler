const SearchUrl = require('../models/searchUrl.model');

class SearchUrlService {
	/**
	 * Tạo hoặc cập nhật search URL từ crawler
	 * @param {string} url - URL của marketplace hoặc bestsellers
	 * @returns {Promise<Object>} SearchUrl object
	 */
	static async createOrUpdateSearchUrl(url) {
		try {
			let searchUrl = await SearchUrl.findOne({ url });

			if (!searchUrl) {
				// Tạo mới
				searchUrl = new SearchUrl({
					url,
				});
			}

			await searchUrl.save();
			return searchUrl;
		} catch (error) {
			console.error('Error creating/updating search URL:', error);
			throw error;
		}
	}

	/**
	 * Cập nhật thời gian crawl cuối cùng
	 * @param {string} url - URL của search
	 */
	static async updateLastCrawledAt(url) {
		try {
			await SearchUrl.findOneAndUpdate(
				{ url },
				{ last_crawled_at: new Date() }
			);
		} catch (error) {
			console.error('Error updating last crawled time:', error);
			throw error;
		}
	}

	/**
	 * Thêm sản phẩm vào search URL
	 * @param {string} url - URL của search
	 * @param {Array} products - Danh sách sản phẩm
	 */
	static async addProductsToSearchUrl(url, products) {
		try {
			const searchUrl = await SearchUrl.findOne({ url });
			if (!searchUrl) {
				throw new Error(`Search URL not found: ${url}`);
			}

			let addedCount = 0;
			let updatedCount = 0;

			for (const product of products) {
				const wasAdded = searchUrl.addProduct(product);
				if (wasAdded) {
					addedCount++;
				} else {
					updatedCount++;
				}
			}

			await searchUrl.save();

			return {
				added: addedCount,
				updated: updatedCount,
				total: searchUrl.products.length,
			};
		} catch (error) {
			console.error('Error adding products to search URL:', error);
			throw error;
		}
	}

	/**
	 * Lấy danh sách search URLs cần crawl
	 * @param {Object} filters - Bộ lọc
	 * @returns {Promise<Array>} Danh sách search URLs
	 */
	static async getSearchUrlsToCrawl(filters = {}) {
		try {
			const searchUrls = await SearchUrl.find(filters)
				.sort({ createdAt: 1 })
				.limit(10);

			return searchUrls;
		} catch (error) {
			console.error('Error getting search URLs to crawl:', error);
			throw error;
		}
	}

	/**
	 * Lấy thống kê crawl
	 * @returns {Promise<Object>} Thống kê
	 */
	static async getCrawlStats() {
		try {
			const stats = await SearchUrl.aggregate([
				{
					$group: {
						_id: null,
						total_urls: { $sum: 1 },
						total_products: { $sum: { $size: '$products' } },
						avg_products_per_url: { $avg: { $size: '$products' } },
					},
				},
			]);

			// Thống kê theo ngày
			const dailyStats = await SearchUrl.aggregate([
				{
					$match: {
						last_crawled_at: { $exists: true, $ne: null },
					},
				},
				{
					$group: {
						_id: {
							$dateToString: {
								format: '%Y-%m-%d',
								date: '$last_crawled_at',
							},
						},
						count: { $sum: 1 },
						products: { $sum: { $size: '$products' } },
					},
				},
				{ $sort: { _id: -1 } },
				{ $limit: 30 },
			]);

			return {
				overview: stats[0] || {
					total_urls: 0,
					total_products: 0,
					avg_products_per_url: 0,
				},
				daily_stats: dailyStats,
			};
		} catch (error) {
			console.error('Error getting crawl stats:', error);
			throw error;
		}
	}

	/**
	 * Xóa search URL và tất cả sản phẩm liên quan
	 * @param {string} url - URL của search
	 */
	static async deleteSearchUrl(url) {
		try {
			const result = await SearchUrl.findOneAndDelete({ url });
			return result;
		} catch (error) {
			console.error('Error deleting search URL:', error);
			throw error;
		}
	}

	/**
	 * Lấy sản phẩm từ search URL theo điều kiện
	 * @param {string} url - URL của search
	 * @param {Object} filters - Bộ lọc sản phẩm
	 * @returns {Promise<Array>} Danh sách sản phẩm
	 */
	static async getProductsFromSearchUrl(url, filters = {}) {
		try {
			const searchUrl = await SearchUrl.findOne({ url });
			if (!searchUrl) {
				return [];
			}

			let products = searchUrl.products;

			// Áp dụng bộ lọc
			if (filters.source) {
				products = products.filter((p) => p.source === filters.source);
			}
			if (filters.asin) {
				products = products.filter((p) => p.asin === filters.asin);
			}
			if (filters.name) {
				products = products.filter((p) =>
					p.name.toLowerCase().includes(filters.name.toLowerCase())
				);
			}

			return products;
		} catch (error) {
			console.error('Error getting products from search URL:', error);
			throw error;
		}
	}

	/**
	 * Cập nhật thời gian crawl cuối cùng
	 * @param {string} url - URL của search
	 */
	static async updateLastCrawlTime(url) {
		try {
			await SearchUrl.findOneAndUpdate(
				{ url },
				{
					last_crawled_at: new Date(),
				}
			);
		} catch (error) {
			console.error('Error updating last crawl time:', error);
			throw error;
		}
	}

	/**
	 * Lấy thông tin sản phẩm từ SearchUrl collection theo URL sản phẩm
	 * @param {string} productUrl - URL của sản phẩm
	 * @returns {Promise<Object|null>} Thông tin sản phẩm hoặc null
	 */
	static async getProductInfoByUrl(productUrl) {
		try {
			// Tìm tất cả SearchUrl có chứa sản phẩm với URL này
			const searchUrls = await SearchUrl.find({
				'products.url': productUrl,
			});

			// Tìm sản phẩm cụ thể trong các SearchUrl
			for (const searchUrl of searchUrls) {
				const product = searchUrl.products.find(
					(p) => p.url === productUrl
				);
				if (product) {
					return {
						asin: product.asin,
						source: product.source,
						rank_b: product.rank_b,
						name: product.name,
					};
				}
			}

			return null;
		} catch (error) {
			console.error('Error getting product info by URL:', error);
			throw error;
		}
	}
}

module.exports = SearchUrlService;

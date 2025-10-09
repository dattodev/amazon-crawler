const SearchUrl = require('../models/searchUrl.model');

// Lấy tất cả search URLs
const getAllSearchUrls = async (req, res) => {
	try {
		const {
			page = 1,
			limit = 10,
			status,
			sortBy = 'createdAt',
			sortOrder = 'desc',
			search,
		} = req.query;

		// Xây dựng filter
		const filter = {};
		if (search) {
			filter.url = { $regex: search, $options: 'i' };
		}

		// Xây dựng sort
		const sort = {};
		sort[sortBy] = sortOrder === 'desc' ? -1 : 1;

		// Tính toán pagination
		const skip = (parseInt(page) - 1) * parseInt(limit);

		// Lấy dữ liệu
		const searchUrls = await SearchUrl.find(filter)
			.sort(sort)
			.skip(skip)
			.limit(parseInt(limit))
			.populate('products')
			.lean();

		// Đếm tổng số
		const total = await SearchUrl.countDocuments(filter);

		// Tính toán thống kê
		const stats = await SearchUrl.aggregate([
			{
				$group: {
					_id: null,
					total_urls: { $sum: 1 },
					total_products: { $sum: { $size: '$products' } },
				},
			},
		]);

		res.json({
			success: true,
			data: searchUrls,
			pagination: {
				current_page: parseInt(page),
				total_pages: Math.ceil(total / parseInt(limit)),
				total_items: total,
				items_per_page: parseInt(limit),
			},
			stats: stats[0] || {
				total_urls: 0,
				total_products: 0,
			},
		});
	} catch (error) {
		console.error('Error getting search URLs:', error);
		res.status(500).json({
			success: false,
			message: 'Error getting search URLs',
			error: error.message,
		});
	}
};

// Lấy search URL theo ID
const getSearchUrlById = async (req, res) => {
	try {
		const { id } = req.params;
		const searchUrl = await SearchUrl.findById(id);

		if (!searchUrl) {
			return res.status(404).json({
				success: false,
				message: 'Search URL not found',
			});
		}

		res.json({
			success: true,
			data: searchUrl,
		});
	} catch (error) {
		console.error('Error getting search URL:', error);
		res.status(500).json({
			success: false,
			message: 'Error getting search URL',
			error: error.message,
		});
	}
};

// Tạo search URL mới
const createSearchUrl = async (req, res) => {
	try {
		const { url, config } = req.body;

		if (!url) {
			return res.status(400).json({
				success: false,
				message: 'URL is required',
			});
		}

		// Kiểm tra xem URL đã tồn tại chưa
		const existingSearchUrl = await SearchUrl.findOne({ url });
		if (existingSearchUrl) {
			return res.status(409).json({
				success: false,
				message: 'Search URL already exists',
				data: existingSearchUrl,
			});
		}

		const searchUrl = new SearchUrl({
			url,
		});

		await searchUrl.save();

		res.status(201).json({
			success: true,
			message: 'Search URL created successfully',
			data: searchUrl,
		});
	} catch (error) {
		console.error('Error creating search URL:', error);
		res.status(500).json({
			success: false,
			message: 'Error creating search URL',
			error: error.message,
		});
	}
};

// Cập nhật search URL
const updateSearchUrl = async (req, res) => {
	try {
		const { id } = req.params;
		const updateData = req.body;

		// Loại bỏ các trường không được phép cập nhật
		delete updateData._id;
		delete updateData.createdAt;
		delete updateData.updatedAt;

		const searchUrl = await SearchUrl.findByIdAndUpdate(id, updateData, {
			new: true,
			runValidators: true,
		});

		if (!searchUrl) {
			return res.status(404).json({
				success: false,
				message: 'Search URL not found',
			});
		}

		res.json({
			success: true,
			message: 'Search URL updated successfully',
			data: searchUrl,
		});
	} catch (error) {
		console.error('Error updating search URL:', error);
		res.status(500).json({
			success: false,
			message: 'Error updating search URL',
			error: error.message,
		});
	}
};

// Xóa search URL
const deleteSearchUrl = async (req, res) => {
	try {
		const { id } = req.params;
		const searchUrl = await SearchUrl.findByIdAndDelete(id);

		if (!searchUrl) {
			return res.status(404).json({
				success: false,
				message: 'Search URL not found',
			});
		}

		res.json({
			success: true,
			message: 'Search URL deleted successfully',
		});
	} catch (error) {
		console.error('Error deleting search URL:', error);
		res.status(500).json({
			success: false,
			message: 'Error deleting search URL',
			error: error.message,
		});
	}
};

// Thêm sản phẩm vào search URL
const addProductToSearchUrl = async (req, res) => {
	try {
		const { id } = req.params;
		const productData = req.body;

		const searchUrl = await SearchUrl.findById(id);
		if (!searchUrl) {
			return res.status(404).json({
				success: false,
				message: 'Search URL not found',
			});
		}

		const wasAdded = searchUrl.addProduct(productData);
		await searchUrl.save();

		res.json({
			success: true,
			message: wasAdded
				? 'Product added successfully'
				: 'Product updated successfully',
			data: searchUrl,
		});
	} catch (error) {
		console.error('Error adding product to search URL:', error);
		res.status(500).json({
			success: false,
			message: 'Error adding product to search URL',
			error: error.message,
		});
	}
};

// Lấy sản phẩm của search URL
const getSearchUrlProducts = async (req, res) => {
	try {
		const { id } = req.params;
		const { source, page = 1, limit = 20 } = req.query;

		const searchUrl = await SearchUrl.findById(id);
		if (!searchUrl) {
			return res.status(404).json({
				success: false,
				message: 'Search URL not found',
			});
		}

		let products = searchUrl.products;

		// Lọc theo source nếu có
		if (source) {
			products = products.filter((p) => p.source === source);
		}

		// Pagination
		const skip = (parseInt(page) - 1) * parseInt(limit);
		const paginatedProducts = products.slice(skip, skip + parseInt(limit));

		res.json({
			success: true,
			data: paginatedProducts,
			pagination: {
				current_page: parseInt(page),
				total_pages: Math.ceil(products.length / parseInt(limit)),
				total_items: products.length,
				items_per_page: parseInt(limit),
			},
		});
	} catch (error) {
		console.error('Error getting search URL products:', error);
		res.status(500).json({
			success: false,
			message: 'Error getting search URL products',
			error: error.message,
		});
	}
};

// Lấy tất cả products từ tất cả search URLs
const getAllProducts = async (req, res) => {
	try {
		const { source, limit = 0, page = 1 } = req.query;

		// Xử lý limit: nếu limit = 0 thì sử dụng số rất lớn để lấy tất cả
		const actualLimit = parseInt(limit) === 0 ? 999999 : parseInt(limit);

		// Xây dựng aggregation pipeline
		const pipeline = [
			// Unwind products array
			{ $unwind: '$products' },
			// Lọc theo source nếu có
			...(source && source !== 'all'
				? [{ $match: { 'products.source': source } }]
				: []),
			// Project để lấy thông tin cần thiết
			{
				$project: {
					_id: '$products._id',
					name: '$products.name',
					asin: '$products.asin',
					url: '$products.url',
					source: '$products.source',
					created_at: '$products.created_at',
					updated_at: '$products.updated_at',
					search_url_id: '$_id',
					search_url: '$url',
				},
			},
		];

		// Thêm pagination
		const skip = (parseInt(page) - 1) * actualLimit;
		pipeline.push({ $skip: skip });
		pipeline.push({ $limit: actualLimit });

		// Thực hiện aggregation
		const products = await SearchUrl.aggregate(pipeline);

		// Đếm tổng số products
		const countPipeline = [
			{ $unwind: '$products' },
			...(source && source !== 'all'
				? [{ $match: { 'products.source': source } }]
				: []),
			{ $count: 'total' },
		];
		const countResult = await SearchUrl.aggregate(countPipeline);
		const total = countResult.length > 0 ? countResult[0].total : 0;

		res.json({
			success: true,
			products: products,
			pagination: {
				current_page: parseInt(page),
				total_pages:
					parseInt(limit) === 0 ? 1 : Math.ceil(total / actualLimit),
				total_items: total,
				items_per_page: parseInt(limit) === 0 ? total : actualLimit,
			},
		});
	} catch (error) {
		console.error('Error getting all products:', error);
		res.status(500).json({
			success: false,
			message: 'Error getting all products',
			error: error.message,
		});
	}
};

// Cập nhật thời gian crawl cuối cùng
const updateLastCrawledAt = async (req, res) => {
	try {
		const { id } = req.params;
		const { last_crawled_at } = req.body;

		const updateData = {};
		if (last_crawled_at) updateData.last_crawled_at = last_crawled_at;

		const searchUrl = await SearchUrl.findByIdAndUpdate(id, updateData, {
			new: true,
		});

		if (!searchUrl) {
			return res.status(404).json({
				success: false,
				message: 'Search URL not found',
			});
		}

		res.json({
			success: true,
			message: 'Last crawled time updated successfully',
			data: searchUrl,
		});
	} catch (error) {
		console.error('Error updating last crawled time:', error);
		res.status(500).json({
			success: false,
			message: 'Error updating last crawled time',
			error: error.message,
		});
	}
};

// Lấy thống kê tổng quan
const getSearchUrlStats = async (req, res) => {
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

		// Thống kê theo source
		const sourceStats = await SearchUrl.aggregate([
			{ $unwind: '$products' },
			{
				$group: {
					_id: '$products.source',
					count: { $sum: 1 },
				},
			},
			{ $sort: { count: -1 } },
		]);

		res.json({
			success: true,
			data: {
				overview: stats[0] || {
					total_urls: 0,
					total_products: 0,
					avg_products_per_url: 0,
				},
				source_breakdown: sourceStats,
			},
		});
	} catch (error) {
		console.error('Error getting search URL stats:', error);
		res.status(500).json({
			success: false,
			message: 'Error getting search URL stats',
			error: error.message,
		});
	}
};

// Lấy thông tin sản phẩm từ SearchUrl theo URL sản phẩm
const getProductInfoByUrl = async (req, res) => {
	try {
		const { productUrl } = req.params;

		if (!productUrl) {
			return res.status(400).json({
				success: false,
				message: 'Product URL is required',
			});
		}

		// Tìm tất cả SearchUrl có chứa sản phẩm với URL này
		const searchUrls = await SearchUrl.find({
			'products.url': productUrl,
		});

		// Tìm sản phẩm cụ thể trong các SearchUrl
		let productInfo = null;
		for (const searchUrl of searchUrls) {
			const product = searchUrl.products.find(
				(p) => p.url === productUrl
			);
			if (product) {
				productInfo = {
					asin: product.asin,
					source: product.source,
					rank_b: product.rank_b,
					name: product.name,
					search_url_id: searchUrl._id,
					search_url: searchUrl.url,
				};
				break;
			}
		}

		if (!productInfo) {
			return res.status(404).json({
				success: false,
				message: 'Product not found in SearchUrl collection',
			});
		}

		res.json({
			success: true,
			data: productInfo,
		});
	} catch (error) {
		console.error('Error getting product info by URL:', error);
		res.status(500).json({
			success: false,
			message: 'Error getting product info by URL',
			error: error.message,
		});
	}
};

module.exports = {
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
};

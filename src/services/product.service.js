const Product = require('../models/product.model');
const ResearchCategory = require('../models/researchCategory.model');

// Ensure a category exists in ResearchCategory collection
async function ensureResearchCategory(categoryName) {
	try {
		if (!categoryName || typeof categoryName !== 'string') return null;
		const slug = String(categoryName)
			.toLowerCase()
			.trim()
			.replace(/[_\s]+/g, '-')
			.replace(/[^a-z0-9-]/g, '-')
			.replace(/-+/g, '-')
			.replace(/^-|-$/g, '');
		if (!slug) return null;
		let existing = await ResearchCategory.findOne({ slug });
		if (existing) return existing;
		return await ResearchCategory.create({ name: categoryName, slug });
	} catch (e) {
		// Best-effort; do not block product creation
		console.warn('ensureResearchCategory failed:', e?.message || e);
		return null;
	}
}

const deleteAllProducts = async () => {
	return await Product.deleteMany();
};

// Hàm trích xuất số từ best_sellers_rank
function extractRankNumber(rankString) {
	if (typeof rankString !== 'string') return Number.MAX_SAFE_INTEGER;

	const match = rankString.match(/#([0-9,]+)/);
	if (match && match[1]) {
		return parseInt(match[1].replace(/,/g, ''), 10);
	}
	return Number.MAX_SAFE_INTEGER;
}

// Hàm chuyển đổi dữ liệu cũ sang cấu trúc mới
function convertToNewRankStructure(oldRankData) {
	if (!oldRankData || !Array.isArray(oldRankData)) {
		return [];
	}

	// Nếu dữ liệu đã ở dạng mới, trả về nguyên vẹn
	if (oldRankData.length > 0 && oldRankData[0].date && oldRankData[0].ranks) {
		return oldRankData;
	}

	// Chuyển đổi từ dạng cũ (mảng string) sang dạng mới
	const newEntry = {
		date: new Date(),
		ranks: [],
	};

	oldRankData.forEach((rankString) => {
		if (typeof rankString === 'string') {
			// Trích xuất category và rank từ string
			const rankNumber = extractRankNumber(rankString);
			if (rankNumber !== Number.MAX_SAFE_INTEGER) {
				// Tìm category trong string
				const categoryMatch = rankString.match(/in\s+([^#]+)/i);
				const category = categoryMatch
					? categoryMatch[1].trim()
					: 'Unknown';

				newEntry.ranks.push({
					category: category,
					rank: rankNumber,
				});
			}
		}
	});

	return [newEntry];
}

// Hàm lấy số thứ tự trong danh mục cụ thể từ mảng best_sellers_rank
function getRankInCategory(rankArray, categoryKeyword) {
	if (!Array.isArray(rankArray) || rankArray.length === 0) {
		return Number.MAX_SAFE_INTEGER;
	}

	// Lấy entry mới nhất (cuối cùng trong mảng)
	const latestEntry = rankArray[rankArray.length - 1];
	if (
		!latestEntry ||
		!latestEntry.ranks ||
		!Array.isArray(latestEntry.ranks)
	) {
		return Number.MAX_SAFE_INTEGER;
	}

	if (categoryKeyword) {
		const categoryRank = latestEntry.ranks.find(
			(r) =>
				r.category &&
				r.category.toLowerCase().includes(categoryKeyword.toLowerCase())
		);
		if (categoryRank && categoryRank.rank) return categoryRank.rank;
	}

	// Nếu không tìm thấy category cụ thể, trả về rank đầu tiên
	return latestEntry.ranks.length > 0
		? latestEntry.ranks[0].rank
		: Number.MAX_SAFE_INTEGER;
}

// Hàm xử lý dữ liệu trước khi lưu
function processProductData(data) {
	const processedData = { ...data };

	// Chuyển đổi best_sellers_rank sang cấu trúc mới nếu cần
	if (data.best_sellers_rank && Array.isArray(data.best_sellers_rank)) {
		processedData.best_sellers_rank = convertToNewRankStructure(
			data.best_sellers_rank
		);

		// Trích xuất các số thứ tự từ best_sellers_rank
		processedData.rank_overall = getRankInCategory(
			processedData.best_sellers_rank
		);
		processedData.rank_kitchen = getRankInCategory(
			processedData.best_sellers_rank,
			'Kitchen & Dining'
		);
		processedData.rank_tumbler = getRankInCategory(
			processedData.best_sellers_rank,
			'Tumblers & Water Glasses'
		);
	}

	return processedData;
}

const createProduct = async (data) => {
	const processedData = processProductData(data);

	// Lấy thông tin từ SearchUrl collection nếu có
	if (processedData.url && processedData.url.trim() !== '') {
		try {
			const SearchUrlService = require('./searchUrl.service');
			const searchUrlInfo = await SearchUrlService.getProductInfoByUrl(
				processedData.url
			);

			if (searchUrlInfo) {
				// Cập nhật thông tin từ SearchUrl nếu chưa có
				if (!processedData.asin && searchUrlInfo.asin) {
					processedData.asin = searchUrlInfo.asin;
				}
				if (!processedData.source && searchUrlInfo.source) {
					processedData.source = searchUrlInfo.source;
				}
				if (
					processedData.rank_b === null &&
					searchUrlInfo.rank_b !== null
				) {
					processedData.rank_b = searchUrlInfo.rank_b;
				}
				console.log(
					`Updated product data from SearchUrl: ASIN=${searchUrlInfo.asin}, source=${searchUrlInfo.source}, rank_b=${searchUrlInfo.rank_b}`
				);
			}
		} catch (error) {
			console.error('Error getting product info from SearchUrl:', error);
		}
	}

	// Kiểm tra duplicate dựa trên ASIN (ưu tiên) hoặc URL (backup)
	let existingProduct = null;

	// Ưu tiên kiểm tra theo ASIN nếu có
	if (processedData.asin && processedData.asin.trim() !== '') {
		existingProduct = await Product.findOne({ asin: processedData.asin });
	}

	// Nếu không tìm thấy theo ASIN, kiểm tra theo URL
	if (
		!existingProduct &&
		processedData.url &&
		processedData.url.trim() !== ''
	) {
		existingProduct = await Product.findOne({ url: processedData.url });
	}

	if (existingProduct) {
		// Nếu sản phẩm đã tồn tại, thêm entry mới vào best_sellers_rank
		if (
			processedData.best_sellers_rank &&
			Array.isArray(processedData.best_sellers_rank)
		) {
			// Thêm các entry mới vào cuối mảng
			existingProduct.best_sellers_rank.push(
				...processedData.best_sellers_rank
			);
		}
		// Cập nhật các trường khác nếu có
		Object.keys(processedData).forEach((key) => {
			if (
				key !== 'best_sellers_rank' &&
				key !== 'asin' &&
				processedData[key] !== undefined
			) {
				existingProduct[key] = processedData[key];
			}
		});
		return await existingProduct.save();
	}

	// Nếu không có ASIN hoặc ASIN rỗng, hoặc không tìm thấy sản phẩm trùng lặp, tạo mới
	// Best-effort: ensure research category exists based on product category
	if (processedData.category) {
		await ensureResearchCategory(processedData.category);
	}
	return await Product.create(processedData);
};

const getAllProducts = async ({
	search,
	category,
	boughtInLast30Days,
	minPrice,
	maxPrice,
	startDate,
	endDate,
	minRank,
	maxRank,
	rankType = 'overall',
	source,
} = {}) => {
	const query = {};
	if (search) {
		query.productName = { $regex: search, $options: 'i' };
	}
	if (category) {
		query.category = category;
	}
	if (source) {
		// Handle multiple source values separated by comma
		if (source.includes(',')) {
			const sourceArray = source.split(',').map((s) => s.trim());
			query.source = { $in: sourceArray };
		} else {
			query.source = source;
		}
	}

	if (boughtInLast30Days) {
		query.boughtInLast30Days = { $gt: 0 };
	}

	if (
		minPrice !== undefined &&
		minPrice !== null &&
		!isNaN(Number(minPrice))
	) {
		query.price = { ...query.price, $gte: Number(minPrice) };
	}

	if (
		maxPrice !== undefined &&
		maxPrice !== null &&
		!isNaN(Number(maxPrice))
	) {
		query.price = { ...query.price, $lte: Number(maxPrice) };
	}

	// Thêm lọc theo thứ hạng
	const rankField = `rank_${rankType}`;
	if (minRank !== undefined && minRank !== null && !isNaN(Number(minRank))) {
		query[rankField] = { ...query[rankField], $gte: Number(minRank) };
	}

	if (maxRank !== undefined && maxRank !== null && !isNaN(Number(maxRank))) {
		query[rankField] = { ...query[rankField], $lte: Number(maxRank) };
	}

	if (startDate) {
		query.date_first_available = {
			...query.date_first_available,
			$gte: new Date(startDate),
		};
	}
	if (endDate) {
		query.date_first_available = {
			...query.date_first_available,
			$lte: new Date(endDate),
		};
	}
	return await Product.find(query);
};

const getProductsCount = async ({
	search,
	category,
	boughtInLast30Days,
	minPrice,
	maxPrice,
	startDate,
	endDate,
	source,
} = {}) => {
	const query = {};
	if (search) {
		query.productName = { $regex: search, $options: 'i' };
	}
	if (category) {
		query.category = category;
	}
	if (source) {
		// Handle multiple source values separated by comma
		if (source.includes(',')) {
			const sourceArray = source.split(',').map((s) => s.trim());
			query.source = { $in: sourceArray };
		} else {
			query.source = source;
		}
	}

	if (boughtInLast30Days) {
		query.boughtInLast30Days = { $gt: 0 };
	}

	if (
		minPrice !== undefined &&
		minPrice !== null &&
		!isNaN(Number(minPrice))
	) {
		query.price = { ...query.price, $gte: Number(minPrice) };
	}

	if (
		maxPrice !== undefined &&
		maxPrice !== null &&
		!isNaN(Number(maxPrice))
	) {
		query.price = { ...query.price, $lte: Number(maxPrice) };
	}

	if (startDate) {
		query.date_first_available = {
			...query.date_first_available,
			$gte: new Date(startDate),
		};
	}
	if (endDate) {
		query.date_first_available = {
			...query.date_first_available,
			$lte: new Date(endDate),
		};
	}
	return await Product.countDocuments(query);
};

const getProductById = async (id) => {
	return await Product.findById(id);
};

const updateProduct = async (id, data) => {
	const processedData = processProductData(data);
	return await Product.findByIdAndUpdate(id, processedData, { new: true });
};

const deleteProduct = async (id) => {
	return await Product.findByIdAndDelete(id);
};

const getUniqueSoldBy = async (sourceFilter = null) => {
	const query = {};
	if (sourceFilter) {
		// Handle multiple source values separated by comma
		if (sourceFilter.includes(',')) {
			const sourceArray = sourceFilter.split(',').map((s) => s.trim());
			query.source = { $in: sourceArray };
		} else {
			query.source = sourceFilter;
		}
	}

	const soldByArr = await Product.find(query, 'soldBy').lean();
	const soldBySet = new Set(soldByArr.map((p) => p.soldBy).filter(Boolean));
	return Array.from(soldBySet);
};

const getUniqueCategories = async (sourceFilter = null) => {
	const query = {};
	if (sourceFilter) {
		// Handle multiple source values separated by comma
		if (sourceFilter.includes(',')) {
			const sourceArray = sourceFilter.split(',').map((s) => s.trim());
			query.source = { $in: sourceArray };
		} else {
			query.source = sourceFilter;
		}
	}

	const categoryArr = await Product.find(query, 'category').lean();
	const categorySet = new Set(
		categoryArr.map((p) => p.category).filter(Boolean)
	);
	return Array.from(categorySet);
};

const getUniqueSources = async () => {
	try {
		const sourceArr = await Product.find({}, 'source').lean();
		const sourceSet = new Set(
			sourceArr.map((p) => p.source).filter(Boolean)
		);
		return Array.from(sourceSet);
	} catch (error) {
		console.error('Error in getUniqueSources:', error);
		throw error;
	}
};

module.exports = {
	createProduct,
	getAllProducts,
	getProductsCount,
	getProductById,
	updateProduct,
	deleteProduct,
	deleteAllProducts,
	getUniqueSoldBy,
	getUniqueCategories,
	getUniqueSources,
};

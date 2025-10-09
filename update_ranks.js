const mongoose = require('mongoose');
require('dotenv').config();

const MONGO_URI =
	process.env.MONGO_URI || 'mongodb://localhost:27017/dashboard';

// Kết nối đến MongoDB
mongoose
	.connect(MONGO_URI, { useNewUrlParser: true, useUnifiedTopology: true })
	.then(() => console.log('Connected to MongoDB'))
	.catch((err) => console.error('MongoDB connection error:', err));

// Import model Product
const Product = require('./src/models/product.model');

// Hàm trích xuất số từ best_sellers_rank
function extractRankNumber(rankString) {
	if (!rankString) return Number.MAX_SAFE_INTEGER;

	// Trích xuất số từ chuỗi kiểu "#123,456 in Category"
	const match = rankString.match(/#([0-9,]+)/);
	if (match && match[1]) {
		// Chuyển đổi chuỗi số có dấu phẩy thành số nguyên
		return parseInt(match[1].replace(/,/g, ''), 10);
	}
	return Number.MAX_SAFE_INTEGER;
}

// Hàm lấy số thứ tự trong danh mục cụ thể từ mảng best_sellers_rank
function getRankInCategory(rankArray, categoryKeyword) {
	if (!rankArray || !Array.isArray(rankArray) || rankArray.length === 0) {
		return Number.MAX_SAFE_INTEGER;
	}

	// Tìm rank trong danh mục cụ thể (nếu có)
	if (categoryKeyword) {
		const categoryRank = rankArray.find((rank) =>
			rank.toLowerCase().includes(categoryKeyword.toLowerCase())
		);
		if (categoryRank) {
			return extractRankNumber(categoryRank);
		}
	}

	// Nếu không tìm thấy danh mục cụ thể hoặc không chỉ định, lấy rank đầu tiên
	return extractRankNumber(rankArray[0]);
}

// Hàm chính để cập nhật dữ liệu
async function updateRanks() {
	try {
		console.log('Starting rank update...');

		// Lấy tất cả sản phẩm
		const products = await Product.find({});
		console.log(`Found ${products.length} products to update`);

		// Đếm số sản phẩm đã cập nhật
		let updatedCount = 0;

		// Cập nhật từng sản phẩm
		for (const product of products) {
			if (
				product.best_sellers_rank &&
				Array.isArray(product.best_sellers_rank)
			) {
				const updates = {
					rank_overall: getRankInCategory(product.best_sellers_rank),
					rank_kitchen: getRankInCategory(
						product.best_sellers_rank,
						'Kitchen & Dining'
					),
					rank_tumbler: getRankInCategory(
						product.best_sellers_rank,
						'Tumblers & Water Glasses'
					),
				};

				await Product.findByIdAndUpdate(product._id, updates);
				updatedCount++;

				if (updatedCount % 10 === 0) {
					console.log(
						`Updated ${updatedCount}/${products.length} products`
					);
				}
			}
		}

		console.log(
			`Completed! Updated ${updatedCount} products with rank information.`
		);
	} catch (error) {
		console.error('Error updating ranks:', error);
	} finally {
		// Đóng kết nối MongoDB
		mongoose.connection.close();
	}
}

// Chạy hàm cập nhật
updateRanks();

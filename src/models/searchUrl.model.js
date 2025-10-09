const mongoose = require('mongoose');

const searchUrlSchema = new mongoose.Schema(
	{
		url: {
			type: String,
			required: true,
			unique: true,
			trim: true,
		},
		last_crawled_at: {
			type: Date,
			default: null,
		},
		products: [
			{
				name: {
					type: String,
					required: true,
				},
				asin: {
					type: String,
					required: true,
				},
				source: {
					type: String,
					enum: [
						'direct',
						'best-sellers',
						'new-releases',
						'best-selling',
						'new-arrivals',
						'marketplace',
						'unknown',
					],
					default: 'unknown',
				},
				url: {
					type: String,
					required: true,
				},
				// Rank B - Số thứ tự trong category Best Sellers/New Releases
				rank_b: {
					type: Number,
					default: null, // null nếu không có rank B
				},
			},
		],
	},
	{
		timestamps: true, // Tự động tạo created_at và updated_at
	}
);

// Index để tối ưu hóa truy vấn
searchUrlSchema.index({ url: 1 });
searchUrlSchema.index({ 'products.asin': 1 });
searchUrlSchema.index({ last_crawled_at: -1 });
searchUrlSchema.index({ createdAt: -1 });

// Virtual để tính tổng số sản phẩm
searchUrlSchema.virtual('totalProducts').get(function () {
	return this.products ? this.products.length : 0;
});

// Method để thêm sản phẩm mới
searchUrlSchema.methods.addProduct = function (productData) {
	// Kiểm tra xem sản phẩm đã tồn tại chưa (dựa trên ASIN thay vì URL)
	const existingProduct = this.products.find(
		(p) => p.asin === productData.asin
	);

	if (!existingProduct) {
		this.products.push({
			...productData,
			created_at: new Date(),
			updated_at: new Date(),
		});
		return true; // Thêm thành công
	} else {
		// Cập nhật sản phẩm hiện có với dữ liệu mới (bao gồm rank_b)
		Object.assign(existingProduct, productData, {
			updated_at: new Date(),
		});
		return false; // Đã tồn tại, chỉ cập nhật
	}
};

// Method để lấy sản phẩm theo source
searchUrlSchema.methods.getProductsBySource = function (source) {
	return this.products.filter((p) => p.source === source);
};

module.exports = mongoose.model('SearchUrl', searchUrlSchema);

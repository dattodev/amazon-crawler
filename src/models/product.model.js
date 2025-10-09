const mongoose = require("mongoose");

const productSchema = new mongoose.Schema(
  {
    productName: { type: String, required: true },
    brand: { type: String },
    brand_table: { type: String }, // Brand từ bảng thông số
    color: { type: String },
    specialFeature: { type: String },
    style: { type: String },
    theme: { type: String },
    soldBy: { type: String },
    shipFrom: { type: String }, // Ships from information
    description: { type: String },
    productImage: { type: String },
    category: { type: String },
    price: { type: Number },
    rating: { type: String },
    totalReviews: { type: String },
    asin: { type: String },
    best_sellers_rank: {
      type: [
        {
          date: { type: Date, default: Date.now },
          ranks: [
            {
              category: { type: String, required: true },
              rank: { type: Number, required: true },
            },
          ],
        },
      ],
      default: [],
    },
    // Thêm các trường mới để lưu số thứ tự đã trích xuất
    rank_overall: { type: Number }, // Số thứ tự tổng thể
    rank_kitchen: { type: Number }, // Số thứ tự trong danh mục Kitchen & Dining
    rank_tumbler: { type: Number }, // Số thứ tự trong danh mục Tumblers
    date_first_available: { type: Date },
    url: { type: String },
    boughtInLast30Days: { type: Number, default: 0 },
    source: {
      type: String,
      enum: [
        "direct",
        "best-sellers",
        "new-releases",
        "best-selling",
        "new-arrivals",
        "unknown",
      ],
      default: "unknown",
    },
    rank: {
      type: Number,
      default: 0,
    },
    // Rank B - Số thứ tự trong category Best Sellers/New Releases
    rank_b: {
      type: Number,
      default: null, // null nếu không có rank B
    },
    // Weight - Trọng lượng sản phẩm
    weight: {
      value: { type: Number }, // Số lượng (1.2)
      unit: { type: String }, // Đơn vị (Pounds, kg, oz, etc.)
      display: { type: String }, // Hiển thị gốc ("1.2 Pounds")
    },
    // Dimensions - Kích thước sản phẩm
    dimensions: {
      length: { type: Number }, // Chiều dài (4.69)
      width: { type: Number }, // Chiều rộng (3.94)
      height: { type: Number }, // Chiều cao (0.75)
      unit: { type: String }, // Đơn vị (inches, cm, etc.)
      display: { type: String }, // Hiển thị gốc ("4.69 x 3.94 x 0.75 inches")
    },
  },
  {
    timestamps: true,
  }
);

module.exports = mongoose.model("Product", productSchema);

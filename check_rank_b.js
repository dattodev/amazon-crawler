const mongoose = require('mongoose');
const Product = require('./src/models/product.model');

mongoose
	.connect('mongodb://localhost:27017/amazon_crawler', {
		useNewUrlParser: true,
		useUnifiedTopology: true,
	})
	.then(async () => {
		console.log('Connected to MongoDB');

		// Tìm sản phẩm có rank_b = null
		const productsWithNullRank = await Product.find({ rank_b: null });
		console.log(
			'Products with rank_b = null:',
			productsWithNullRank.length
		);

		if (productsWithNullRank.length > 0) {
			console.log('Sample product with null rank_b:');
			console.log({
				_id: productsWithNullRank[0]._id,
				productName: productsWithNullRank[0].productName,
				url: productsWithNullRank[0].url,
				rank_b: productsWithNullRank[0].rank_b,
				source: productsWithNullRank[0].source,
			});
		}

		// Tìm sản phẩm có rank_b khác null
		const productsWithRank = await Product.find({ rank_b: { $ne: null } });
		console.log('Products with rank_b != null:', productsWithRank.length);

		if (productsWithRank.length > 0) {
			console.log('Sample product with rank_b:');
			console.log({
				_id: productsWithRank[0]._id,
				productName: productsWithRank[0].productName,
				url: productsWithRank[0].url,
				rank_b: productsWithRank[0].rank_b,
				source: productsWithRank[0].source,
			});
		}

		process.exit(0);
	})
	.catch((err) => {
		console.error('Error:', err);
		process.exit(1);
	});

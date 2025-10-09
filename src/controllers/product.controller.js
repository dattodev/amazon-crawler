const productService = require('../services/product.service');

exports.deleteAllProducts = async (req, res) => {
	try {
		await productService.deleteAllProducts();
		res.json({ message: 'All products deleted' });
	} catch (err) {
		res.status(500).json({ error: err.message });
	}
};

exports.getUniqueSoldBy = async (req, res) => {
	try {
		const { source } = req.query;
		const soldByList = await productService.getUniqueSoldBy(source);
		res.json(soldByList);
	} catch (err) {
		res.status(500).json({ error: err.message });
	}
};

exports.getUniqueCategories = async (req, res) => {
	try {
		const { source } = req.query;
		const categoryList = await productService.getUniqueCategories(source);
		res.json(categoryList);
	} catch (err) {
		res.status(500).json({ error: err.message });
	}
};

exports.getUniqueSources = async (req, res) => {
	try {
		console.log('Getting unique sources...');

		// First check if we have any products at all
		const allProducts = await productService.getAllProducts();
		console.log('Total products in database:', allProducts.length);

		if (allProducts.length === 0) {
			console.log('No products found in database');
			return res.json([]);
		}

		// Check if any products have source field
		const productsWithSource = allProducts.filter((p) => p.source);
		console.log('Products with source field:', productsWithSource.length);

		const sourceList = await productService.getUniqueSources();
		console.log('Source list:', sourceList);
		res.json(sourceList);
	} catch (err) {
		console.error('Error in getUniqueSources controller:', err);
		res.status(500).json({ error: err.message });
	}
};

exports.createProduct = async (req, res) => {
	console.log('req.body:', req.body);
	try {
		const product = await productService.createProduct(req.body);
		res.status(201).json(product);
	} catch (err) {
		res.status(400).json({ error: err.message });
	}
};

exports.getAllProducts = async (req, res) => {
	try {
		const {
			search,
			category,
			boughtInLast30Days,
			minPrice,
			maxPrice,
			startDate,
			endDate,
			source,
		} = req.query;

		console.log('req.query:', req.query);
		const products = await productService.getAllProducts({
			search,
			category,
			boughtInLast30Days,
			minPrice,
			maxPrice,
			startDate,
			endDate,
			source,
		});
		res.json(products);
	} catch (err) {
		res.status(500).json({ error: err.message });
	}
};

exports.getProductsCount = async (req, res) => {
	try {
		const {
			search,
			category,
			boughtInLast30Days,
			minPrice,
			maxPrice,
			startDate,
			endDate,
			source,
		} = req.query;

		console.log('req.query for count:', req.query);
		const count = await productService.getProductsCount({
			search,
			category,
			boughtInLast30Days,
			minPrice,
			maxPrice,
			startDate,
			endDate,
			source,
		});
		res.json({ count });
	} catch (err) {
		res.status(500).json({ error: err.message });
	}
};

exports.getProductById = async (req, res) => {
	try {
		const product = await productService.getProductById(req.params.id);
		if (!product)
			return res.status(404).json({ error: 'Product not found' });
		res.json(product);
	} catch (err) {
		res.status(500).json({ error: err.message });
	}
};

exports.updateProduct = async (req, res) => {
	try {
		const product = await productService.updateProduct(
			req.params.id,
			req.body
		);
		if (!product)
			return res.status(404).json({ error: 'Product not found' });
		res.json(product);
	} catch (err) {
		res.status(400).json({ error: err.message });
	}
};

exports.deleteProduct = async (req, res) => {
	try {
		const product = await productService.deleteProduct(req.params.id);
		if (!product)
			return res.status(404).json({ error: 'Product not found' });
		res.json({ message: 'Product deleted' });
	} catch (err) {
		res.status(500).json({ error: err.message });
	}
};

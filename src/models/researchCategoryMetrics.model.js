const mongoose = require('mongoose');

const researchCategoryMetricsSchema = new mongoose.Schema(
	{
		datasetId: {
			type: mongoose.Schema.Types.ObjectId,
			ref: 'ResearchDataset',
			required: true,
		},
		categoryId: {
			type: mongoose.Schema.Types.ObjectId,
			ref: 'ResearchCategory',
			required: true,
		},
		metrics: { type: Map, of: Number, default: {} },
	},
	{ timestamps: true }
);

researchCategoryMetricsSchema.index(
	{ datasetId: 1, categoryId: 1 },
	{ unique: true }
);

module.exports = mongoose.model(
	'ResearchCategoryMetrics',
	researchCategoryMetricsSchema
);

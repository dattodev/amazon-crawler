const mongoose = require('mongoose');

const researchSeriesSchema = new mongoose.Schema(
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
		metric: { type: String, required: true },
		bucket: { type: String, required: true }, // YYYY-MM or YYYY-QN
		value: { type: Number, required: true },
		unit: {
			type: String,
			enum: ['usd', 'pct', 'units', 'count', 'ratio'],
			default: 'units',
		},
		sourceSheet: { type: String },
		sampleSize: { type: Number },
		sampleType: { type: String },
	},
	{ timestamps: true }
);

researchSeriesSchema.index({ categoryId: 1, metric: 1, bucket: 1 });
researchSeriesSchema.index({ datasetId: 1, metric: 1, bucket: 1 });

module.exports = mongoose.model('ResearchSeries', researchSeriesSchema);

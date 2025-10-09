const mongoose = require('mongoose');

const sheetInfoSchema = new mongoose.Schema(
	{
		sheetName: { type: String, required: true },
		detectedType: { type: String },
		columns: { type: [String], default: [] },
		rows: { type: Number, default: 0 },
	},
	{ _id: false }
);

const researchDatasetSchema = new mongoose.Schema(
	{
		categoryId: {
			type: mongoose.Schema.Types.ObjectId,
			ref: 'ResearchCategory',
			required: true,
		},
		originalFilename: { type: String, required: true },
		storagePath: { type: String },
		workbookData: { type: Buffer }, // For serverless environments
		sheets: { type: [sheetInfoSchema], default: [] },
		timeRange: {
			from: { type: String }, // YYYY-MM
			to: { type: String }, // YYYY-MM
			bucket: {
				type: String,
				enum: ['month', 'quarter'],
				default: 'month',
			},
		},
		status: {
			type: String,
			enum: ['uploaded', 'parsed', 'ready', 'failed'],
			default: 'uploaded',
		},
		notes: { type: String },
	},
	{ timestamps: true }
);

researchDatasetSchema.index({ categoryId: 1, createdAt: -1 });
researchDatasetSchema.index({ status: 1 });

module.exports = mongoose.model('ResearchDataset', researchDatasetSchema);

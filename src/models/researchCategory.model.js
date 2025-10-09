const mongoose = require('mongoose');

// Function to generate slug from name
function generateSlug(name) {
	return name
		.toLowerCase()
		.replace(/[^a-z0-9\s-]/g, '') // Remove special characters
		.replace(/\s+/g, '-') // Replace spaces with hyphens
		.replace(/-+/g, '-') // Replace multiple hyphens with single
		.trim('-'); // Remove leading/trailing hyphens
}

const researchCategorySchema = new mongoose.Schema(
	{
		name: { type: String, required: true },
		slug: { type: String, unique: true },
		description: { type: String },
		// FBA and size tier estimates
		fbaFeeUsd: { type: Number },
		sizeTierEstimate: { type: String },
		avgWeightLb: { type: Number },
		avgVolumeIn3: { type: Number },
		estimatedSideIn: { type: Number },
		estimatedDimensionalWeightLb: { type: Number },
		estimatedShippingWeightLb: { type: Number },
		// Referral fee defaults
		referralFeePercentDefault: { type: Number },
		referralMinFeeUsd: { type: Number },
		// Ads metrics defaults
		defaultCtr: { type: Number },
		defaultCpc: { type: Number },
		defaultRoas: { type: Number },
		defaultCr: { type: Number },
		defaultAcos: { type: Number },
		defaultTacos: { type: Number },
		defaultCpp: { type: Number },
	},
	{ timestamps: true }
);

// Auto-generate slug before saving
researchCategorySchema.pre('save', async function (next) {
	try {
		// Always generate slug if not provided
		if (!this.slug) {
			let baseSlug = generateSlug(this.name);
			let slug = baseSlug;
			let counter = 1;

			// Ensure uniqueness
			while (await this.constructor.findOne({ slug })) {
				slug = `${baseSlug}-${counter}`;
				counter++;
			}

			this.slug = slug;
		}
		next();
	} catch (error) {
		next(error);
	}
});

researchCategorySchema.index({ slug: 1 }, { unique: true });

module.exports = mongoose.model('ResearchCategory', researchCategorySchema);

const express = require('express');
const multer = require('multer');
const path = require('path');
const fs = require('fs');
const XLSX = require('xlsx');

const ResearchCategory = require('../models/researchCategory.model');
const ResearchDataset = require('../models/researchDataset.model');
const ResearchSeries = require('../models/researchSeries.model');
const ReferralFeeRule = require('../models/referralFeeRule.model');
const SizeTierRule = require('../models/sizeTierRule.model');
const FbaFeeRule = require('../models/fbaFeeRule.model');

const router = express.Router();

// Helper function to handle Market Analysis sheet
async function handleMarketAnalysisSheet(dataset, rows, header, res) {
	try {
		// Preload latest FBA fee for this category (overall constant)
		let latestFbaFeeUsd = 0;
		try {
			const fba = await ResearchSeries.findOne({
				categoryId: dataset.categoryId,
				metric: 'fba_fee',
			})
				.sort({ createdAt: -1 })
				.lean();
			if (fba && typeof fba.value === 'number')
				latestFbaFeeUsd = fba.value;
			if (!latestFbaFeeUsd) {
				const cat = await ResearchCategory.findById(
					dataset.categoryId
				).lean();
				if (cat && typeof cat.fbaFeeUsd === 'number')
					latestFbaFeeUsd = cat.fbaFeeUsd;
			}
		} catch (e) {
			latestFbaFeeUsd = 0;
		}
		// Find the "Avg. Monthly Unit Sales" column
		const salesColIdx = header.findIndex(
			(h) =>
				h.toLowerCase().includes('avg. monthly unit sales') ||
				h.toLowerCase().includes('avg monthly unit sales')
		);

		if (salesColIdx < 0) {
			return res.status(400).json({
				error: 'Avg. Monthly Unit Sales column not found in Market Analysis sheet',
			});
		}

		// Find the "Avg. Monthly Revenue($)" column
		const revenueColIdx = header.findIndex(
			(h) =>
				h.toLowerCase().includes('avg. monthly revenue') ||
				h.toLowerCase().includes('avg monthly revenue') ||
				h.toLowerCase().includes('avg. monthly revenue($)')
		);

		if (revenueColIdx < 0) {
			return res.status(400).json({
				error: 'Avg. Monthly Revenue($) column not found in Market Analysis sheet',
			});
		}

		// Find time column (look for common time patterns)
		const timeColIdx = header.findIndex(
			(h) =>
				h.toLowerCase().includes('month') ||
				h.toLowerCase().includes('date') ||
				h.toLowerCase().includes('period') ||
				h.toLowerCase().includes('time')
		);

		if (timeColIdx < 0) {
			return res.status(400).json({
				error: 'Time column not found in Market Analysis sheet',
			});
		}

		// Find Sample Size column
		const sampleSizeColIdx = header.findIndex(
			(h) =>
				h.toLowerCase().includes('sample size') ||
				h.toLowerCase().includes('sample_size') ||
				h.toLowerCase().includes('samplesize')
		);

		if (sampleSizeColIdx < 0) {
			return res.status(400).json({
				error: 'Sample Size column not found in Market Analysis sheet',
			});
		}

		// Find Sample Type column
		const sampleTypeColIdx = header.findIndex(
			(h) =>
				h.toLowerCase().includes('sample type') ||
				h.toLowerCase().includes('sample_type') ||
				h.toLowerCase().includes('sampletype')
		);

		if (sampleTypeColIdx < 0) {
			return res.status(400).json({
				error: 'Sample Type column not found in Market Analysis sheet',
			});
		}

		// Find Avg. Price($) column
		const priceColIdx = header.findIndex(
			(h) =>
				h.toLowerCase().includes('avg. price') ||
				h.toLowerCase().includes('avg price') ||
				h.toLowerCase().includes('avg. price($)')
		);

		if (priceColIdx < 0) {
			return res.status(400).json({
				error: 'Avg. Price($) column not found in Market Analysis sheet',
			});
		}

		// Find Avg. Ratings column
		const ratingsColIdx = header.findIndex((h) => {
			const l = h.toLowerCase();
			return (
				l.includes('avg. ratings') ||
				l.includes('avg ratings') ||
				/\bavg\.?\s+ratings\b/.test(l)
			);
		});

		if (ratingsColIdx < 0) {
			return res.status(400).json({
				error: 'Avg. Ratings column not found in Market Analysis sheet',
			});
		}

		// Find Avg. Rating column (singular, not Ratings)
		const ratingColIdx = header.findIndex((h) => {
			const l = h.toLowerCase();
			return (
				(l.includes('avg. rating') ||
					l.includes('avg rating') ||
					/\bavg\.?\s+rating\b/.test(l)) &&
				!l.includes('ratings')
			);
		});

		if (ratingColIdx < 0) {
			return res.status(400).json({
				error: 'Avg. Rating column not found in Market Analysis sheet',
			});
		}

		const docs = [];

		// Process each row (skip header)
		for (let r = 1; r < rows.length; r++) {
			const row = rows[r];
			const rawTime = row[timeColIdx];
			const rawSales = row[salesColIdx];
			const rawRevenue = row[revenueColIdx];
			const rawSampleSize = row[sampleSizeColIdx];
			const rawSampleType = row[sampleTypeColIdx];
			const rawPrice = row[priceColIdx];
			const rawRatings = row[ratingsColIdx];
			const rawRating = row[ratingColIdx];

			if (
				rawTime == null ||
				rawSales == null ||
				rawRevenue == null ||
				rawSampleSize == null ||
				rawSampleType == null ||
				rawPrice == null ||
				rawRatings == null ||
				rawRating == null
			)
				continue;

			// Use dataset timeRange.from from filename instead of Time column
			// This ensures consistent time bucket format across all metrics
			let bucket = dataset.timeRange?.from || 'overall';

			// Parse sales value
			let salesValue = rawSales;
			if (typeof salesValue === 'string') {
				// Remove commas and other formatting
				salesValue = parseFloat(salesValue.replace(/[,$\s]/g, ''));
			}

			// Parse revenue value
			let revenueValue = rawRevenue;
			if (typeof revenueValue === 'string') {
				// Remove commas and other formatting
				revenueValue = parseFloat(revenueValue.replace(/[,$\s]/g, ''));
			}

			// Parse sample size value
			let sampleSize = rawSampleSize;
			if (typeof sampleSize === 'string') {
				// Remove commas and other formatting
				sampleSize = parseFloat(sampleSize.replace(/[,$\s]/g, ''));
			}

			// Parse price value
			let priceValue = rawPrice;
			if (typeof priceValue === 'string') {
				// Remove dollar signs, commas and other formatting
				priceValue = parseFloat(priceValue.replace(/[$,\s]/g, ''));
			}

			// Parse ratings value
			let ratingsValue = rawRatings;
			if (typeof ratingsValue === 'string') {
				// Remove commas and other formatting
				ratingsValue = parseFloat(ratingsValue.replace(/[,\s]/g, ''));
			}

			// Parse rating value
			let ratingValue = rawRating;
			if (typeof ratingValue === 'string') {
				// Remove commas and other formatting
				ratingValue = parseFloat(ratingValue.replace(/[,\s]/g, ''));
			}

			if (
				isNaN(salesValue) ||
				salesValue <= 0 ||
				isNaN(revenueValue) ||
				revenueValue <= 0 ||
				isNaN(sampleSize) ||
				sampleSize <= 0 ||
				isNaN(priceValue) ||
				priceValue <= 0 ||
				isNaN(ratingsValue) ||
				ratingsValue <= 0 ||
				isNaN(ratingValue) ||
				ratingValue <= 0
			)
				continue;

			// Calculate Sales (units) = Avg. Monthly Unit Sales * Sample Size
			const calculatedSales = salesValue * sampleSize;

			// Calculate Revenue ($) = Avg. Monthly Revenue($) * 100
			const calculatedRevenue = revenueValue * 100;

			// Add Sales (units) record
			docs.push({
				datasetId: dataset._id,
				categoryId: dataset.categoryId,
				metric: 'sales_units',
				bucket,
				value: calculatedSales,
				unit: 'units',
				sourceSheet: 'Market Analysis',
				sampleSize: sampleSize,
				sampleType: String(rawSampleType).trim(),
			});

			// Add Revenue ($) record
			docs.push({
				datasetId: dataset._id,
				categoryId: dataset.categoryId,
				metric: 'revenue',
				bucket,
				value: calculatedRevenue,
				unit: 'usd',
				sourceSheet: 'Market Analysis',
				sampleSize: 100, // Revenue uses fixed 100 multiplier
				sampleType: String(rawSampleType).trim(),
			});

			// Add Avg. Price($) record
			docs.push({
				datasetId: dataset._id,
				categoryId: dataset.categoryId,
				metric: 'avg_price',
				bucket,
				value: priceValue,
				unit: 'usd',
				sourceSheet: 'Market Analysis',
				sampleSize: sampleSize,
				sampleType: String(rawSampleType).trim(),
			});

			// Compute Referral Fee using referral rules (dashboard logic)
			let referralFeeValue = null;
			let referralFeePercent = null;
			try {
				const cat = await ResearchCategory.findById(
					dataset.categoryId
				).lean();
				if (cat && cat.name) {
					const rules = await ReferralFeeRule.find({}).lean();

					// Find matching rules for this category (dashboard logic)
					const matchingRules = rules.filter((rule) => {
						// Normalize category strings (case-insensitive, & vs and, remove non-letters)
						const normalize = (str) =>
							String(str || '')
								.toLowerCase()
								.replace(/&/g, ' and ')
								.replace(/[^a-z0-9]+/g, ' ')
								.replace(/\s+/g, ' ')
								.trim();
						const tokens = (str) =>
							new Set(normalize(str).split(' ').filter(Boolean));
						const jaccard = (a, b) => {
							const A = tokens(a);
							const B = tokens(b);
							if (A.size === 0 || B.size === 0) return 0;
							let inter = 0;
							A.forEach((t) => {
								if (B.has(t)) inter++;
							});
							const union = A.size + B.size - inter;
							return inter / union;
						};

						const ruleCategoryNorm = normalize(rule.category);
						const productCategoryNorm = normalize(cat.name);

						// Match if substring either way OR token similarity high enough
						const substringMatch =
							productCategoryNorm.includes(ruleCategoryNorm) ||
							ruleCategoryNorm.includes(productCategoryNorm);
						const similarity = jaccard(rule.category, cat.name);
						const categoryMatch =
							(ruleCategoryNorm.length > 0 && substringMatch) ||
							similarity >= 0.5;

						if (!categoryMatch) return false;

						// Check price range
						const priceMin = rule.priceMin || 0;
						const priceMax =
							rule.priceMax !== 0 && rule.priceMax !== null
								? rule.priceMax
								: Infinity;

						const priceMatch =
							priceValue >= priceMin && priceValue <= priceMax;
						return priceMatch;
					});

					if (matchingRules.length > 0) {
						// Sort rules by price range (prefer more specific ranges)
						matchingRules.sort((a, b) => {
							const aRange =
								(a.priceMax || Infinity) - (a.priceMin || 0);
							const bRange =
								(b.priceMax || Infinity) - (b.priceMin || 0);
							return aRange - bRange;
						});

						// Calculate fee based on Apply_To type (dashboard logic)
						let totalFee = 0;
						for (const rule of matchingRules) {
							const applyTo =
								rule.applyTo?.toLowerCase() || 'total';
							if (applyTo === 'total') {
								// Apply_To = Total: Fee = Price x Referral_Rate
								const fee = priceValue * (rule.feePercent || 0);
								totalFee += fee;
							} else if (applyTo === 'portion') {
								// Apply_To = Portion: Calculate fee for the portion of price that falls within this rule's range
								const rulePriceMin = rule.priceMin || 0;
								const rulePriceMax = rule.priceMax || Infinity;

								const portionStart = Math.max(rulePriceMin, 0);
								const portionEnd = Math.min(
									priceValue,
									rulePriceMax
								);
								const portionAmount = Math.max(
									0,
									portionEnd - portionStart
								);

								if (portionAmount > 0) {
									const fee =
										portionAmount * (rule.feePercent || 0);
									totalFee += fee;
								}
							}
						}

						// Apply minimum fee if specified
						const minFee = Math.max(
							...matchingRules.map((rule) => rule.minFeeUSD || 0)
						);

						if (minFee > 0) {
							totalFee = Math.max(totalFee, minFee);
						}

						if (totalFee > 0) {
							referralFeeValue = totalFee;
							referralFeePercent = matchingRules[0].feePercent; // Use first rule's percent for display
						}
					} else {
						// Fallback: use category-level default percent/min if available
						const defaultPct = Number(
							cat.referralFeePercentDefault
						);
						const minFee = Number(cat.referralMinFeeUsd || 0);
						if (!Number.isNaN(defaultPct) && defaultPct > 0) {
							const fee = priceValue * defaultPct;
							referralFeeValue = Math.max(fee, minFee);
							referralFeePercent = defaultPct;
						}
					}
				}
			} catch (e) {
				// swallow referral fee error, continue
			}

			if (referralFeeValue != null) {
				docs.push({
					datasetId: dataset._id,
					categoryId: dataset.categoryId,
					metric: 'referral_fee',
					bucket,
					value: referralFeeValue,
					unit: 'usd',
					sourceSheet: 'Market Analysis',
					sampleSize: sampleSize,
					sampleType: String(rawSampleType).trim(),
					feePercent: referralFeePercent,
					basePrice: priceValue,
				});
			}

			// Derived metrics per dataset month using defaults (ads 20%, profit target 20%)
			try {
				const ads = 0.2 * priceValue;
				const profitTarget = 0.2 * priceValue;
				const feeSum =
					(Number(referralFeeValue) || 0) +
					(Number(latestFbaFeeUsd) || 0);
				const cogsCap = priceValue - (ads + feeSum + profitTarget);

				// Profit uses assumed COGS = 20% price
				const cogsAssumed = 0.2 * priceValue;
				const profitDollar = priceValue - (ads + feeSum + cogsAssumed);
				const marginPct =
					priceValue > 0 ? (profitDollar / priceValue) * 100 : 0;
				const roiPct =
					cogsAssumed > 0 ? (profitDollar / cogsAssumed) * 100 : 0;

				docs.push({
					datasetId: dataset._id,
					categoryId: dataset.categoryId,
					metric: 'cogs_cap',
					bucket,
					value: cogsCap,
					unit: 'usd',
					sourceSheet: 'Derived',
				});

				docs.push({
					datasetId: dataset._id,
					categoryId: dataset.categoryId,
					metric: 'profit',
					bucket,
					value: profitDollar,
					unit: 'usd',
					sourceSheet: 'Derived',
				});

				docs.push({
					datasetId: dataset._id,
					categoryId: dataset.categoryId,
					metric: 'margin',
					bucket,
					value: marginPct,
					unit: 'pct',
					sourceSheet: 'Derived',
				});

				docs.push({
					datasetId: dataset._id,
					categoryId: dataset.categoryId,
					metric: 'roi',
					bucket,
					value: roiPct,
					unit: 'pct',
					sourceSheet: 'Derived',
				});
			} catch (e) {
				// ignore derived compute failures and continue
			}

			// Add Avg. Ratings record
			docs.push({
				datasetId: dataset._id,
				categoryId: dataset.categoryId,
				metric: 'avg_ratings',
				bucket,
				value: ratingsValue,
				unit: 'count',
				sourceSheet: 'Market Analysis',
				sampleSize: sampleSize,
				sampleType: String(rawSampleType).trim(),
			});

			// Add Avg. Rating record
			docs.push({
				datasetId: dataset._id,
				categoryId: dataset.categoryId,
				metric: 'avg_rating',
				bucket,
				value: ratingValue,
				unit: 'count',
				sourceSheet: 'Market Analysis',
				sampleSize: sampleSize,
				sampleType: String(rawSampleType).trim(),
			});
		}

		if (docs.length === 0) {
			return res.status(400).json({
				error: 'No valid data found in Market Analysis sheet',
			});
		}

		// Clear existing data for this sheet
		await ResearchSeries.deleteMany({
			datasetId: dataset._id,
			sourceSheet: 'Market Analysis',
		});

		// Insert new data
		await ResearchSeries.insertMany(docs);

		// Update dataset status
		await ResearchDataset.updateOne(
			{ _id: dataset._id },
			{ $set: { status: 'ready' } }
		);

		// Group data by sample type for display
		const groupedData = {};
		docs.forEach((doc) => {
			const key = doc.sampleType;
			if (!groupedData[key]) {
				groupedData[key] = {
					sampleType: doc.sampleType,
					sales: null,
					revenue: null,
					avgPrice: null,
					avgRatings: null,
					avgRating: null,
					referralFee: null,
				};
			}

			if (doc.metric === 'sales_units') {
				groupedData[key].sales = {
					originalValue: doc.value / doc.sampleSize,
					sampleSize: doc.sampleSize,
					calculatedValue: doc.value,
					formula: `${doc.value / doc.sampleSize} × ${
						doc.sampleSize
					} = ${doc.value}`,
				};
			} else if (doc.metric === 'revenue') {
				groupedData[key].revenue = {
					originalValue: doc.value / 100,
					sampleSize: 100,
					calculatedValue: doc.value,
					formula: `${doc.value / 100} × 100 = ${doc.value}`,
				};
			} else if (doc.metric === 'avg_price') {
				groupedData[key].avgPrice = {
					originalValue: doc.value,
					sampleSize: doc.sampleSize,
					calculatedValue: doc.value,
					formula: `Value: ${doc.value}`,
				};
			} else if (doc.metric === 'avg_ratings') {
				groupedData[key].avgRatings = {
					originalValue: doc.value,
					sampleSize: doc.sampleSize,
					calculatedValue: doc.value,
					formula: `Value: ${doc.value}`,
				};
			} else if (doc.metric === 'avg_rating') {
				groupedData[key].avgRating = {
					originalValue: doc.value,
					sampleSize: doc.sampleSize,
					calculatedValue: doc.value,
					formula: `Value: ${doc.value}`,
				};
			} else if (doc.metric === 'referral_fee') {
				groupedData[key].referralFee = {
					originalValue: doc.value,
					calculatedValue: doc.value,
					feePercent: doc.feePercent ?? null,
					avgPrice: doc.basePrice ?? null,
					formula: `Referral Fee = Avg. Price × Fee%`,
				};
			}
		});

		// Fill referral fee message when no rule matched
		Object.keys(groupedData).forEach((k) => {
			if (!groupedData[k].referralFee) {
				groupedData[k].referralFee = { message: 'No category rule' };
			}
		});

		res.json({
			inserted: docs.length,
			message: `Processed ${
				docs.length
			} records from Market Analysis sheet (${
				Object.keys(groupedData).length
			} sample types)`,
			calculation:
				'Sales (units) = Avg. Monthly Unit Sales × Sample Size | Revenue ($) = Avg. Monthly Revenue($) × 100 | Avg. Price($) = Avg. Price($) | Avg. Ratings = Avg. Ratings | Avg. Rating = Avg. Rating | Referral Fee = Avg. Price × Fee%',
			processedData: Object.values(groupedData),
		});
	} catch (e) {
		console.error('Market Analysis processing failed:', e);
		res.status(500).json({
			error: 'Failed to process Market Analysis sheet',
		});
	}
}

// Helper function to handle Fulfillment sheet
async function handleFulfillmentSheet(dataset, rows, header, res) {
	try {
		const fulfillIdx = header.findIndex((h) =>
			String(h).toLowerCase().includes('fulfillment')
		);
		const propIdx = header.findIndex((h) => {
			const l = String(h).toLowerCase();
			return (
				l.includes('asins proportion') ||
				(l.includes('asin') && l.includes('proportion'))
			);
		});

		if (fulfillIdx < 0 || propIdx < 0) {
			return res.status(400).json({
				error: 'Fulfillment or ASINs Proportion column not found in Fulfillment sheet',
			});
		}

		const docs = [];
		for (let r = 1; r < rows.length; r++) {
			const row = rows[r];
			const rawType = row[fulfillIdx];
			const rawProp = row[propIdx];
			if (
				rawType == null ||
				rawType === '' ||
				rawProp == null ||
				rawProp === ''
			)
				continue;

			const type = String(rawType).trim().toLowerCase();
			let code = type.replace(/[^a-z]/g, '');
			if (code.includes('fba')) code = 'fba';
			else if (code.includes('fbm')) code = 'fbm';
			else if (code.includes('amz')) code = 'amz';
			else if (code === 'na') code = 'na';

			let pct = rawProp;
			if (typeof pct === 'string') {
				pct = parseFloat(pct.replace(/[%\s,]/g, ''));
			}
			if (typeof pct !== 'number' || Number.isNaN(pct)) continue;
			// Normalize: if value is in 0-1, convert to percentage points
			if (pct > 0 && pct <= 1) pct = pct * 100;

			docs.push({
				datasetId: dataset._id,
				categoryId: dataset.categoryId,
				metric: `fulfillment_${code}`,
				bucket: 'overall',
				value: pct,
				unit: 'pct',
				sourceSheet: 'Fulfillment',
			});
		}

		if (!docs.length) {
			return res
				.status(400)
				.json({ error: 'No valid rows found in Fulfillment sheet' });
		}

		await ResearchSeries.deleteMany({
			datasetId: dataset._id,
			sourceSheet: 'Fulfillment',
		});
		await ResearchSeries.insertMany(docs);
		await ResearchDataset.updateOne(
			{ _id: dataset._id },
			{ $set: { status: 'ready' } }
		);

		res.json({
			inserted: docs.length,
			message: `Processed ${docs.length} rows from Fulfillment sheet`,
			processedData: docs.map((d) => ({
				metric: d.metric,
				value: d.value,
				unit: d.unit,
			})),
		});
	} catch (e) {
		console.error('Fulfillment processing failed:', e);
		res.status(500).json({ error: 'Failed to process Fulfillment sheet' });
	}
}

// Helper function to handle Publication Time sheet
async function handlePublicationTimeSheet(dataset, rows, header, res) {
	try {
		const timeIdx = header.findIndex((h) =>
			String(h).toLowerCase().includes('publication time')
		);
		const salesPropIdx = header.findIndex((h) => {
			const l = String(h).toLowerCase();
			return l.includes('sales proportion');
		});
		if (timeIdx < 0 || salesPropIdx < 0) {
			return res.status(400).json({
				error: 'Publication Time or Sales Proportion column not found',
			});
		}

		let totalPct = 0;
		let newPct = 0;
		const detailRows = [];
		for (let r = 1; r < rows.length; r++) {
			const row = rows[r];
			const rawBucket = row[timeIdx];
			const rawSalesProp = row[salesPropIdx];
			if (rawBucket == null || rawSalesProp == null) continue;
			const labelOriginal = String(rawBucket).trim();
			const label = labelOriginal.toLowerCase();
			let pct = rawSalesProp;
			if (typeof pct === 'string')
				pct = parseFloat(pct.replace(/[%\s,]/g, ''));
			if (typeof pct !== 'number' || Number.isNaN(pct)) continue;
			if (pct > 0 && pct <= 1) pct = pct * 100; // normalize if 0-1
			totalPct += pct;
			// New = month-based buckets only (exclude any year entries)
			const isNew =
				/\bmonth\b|\bmonths\b/.test(label) && !label.includes('year');
			if (isNew) newPct += pct;
			detailRows.push({
				publicationTime: labelOriginal,
				salesProportion: pct,
				isNew,
			});
		}

		if (totalPct === 0) {
			return res
				.status(400)
				.json({ error: 'No valid Sales Proportion values found' });
		}

		// New product ratio (%) is the sum of Sales Proportion for month buckets
		const ratio = newPct;

		const docs = [
			{
				datasetId: dataset._id,
				categoryId: dataset.categoryId,
				metric: 'new_product_ratio',
				bucket: 'overall',
				value: ratio,
				unit: 'pct',
				sourceSheet: 'Publication Time',
			},
		];

		await ResearchSeries.deleteMany({
			datasetId: dataset._id,
			sourceSheet: 'Publication Time',
		});
		await ResearchSeries.insertMany(docs);
		await ResearchDataset.updateOne(
			{ _id: dataset._id },
			{ $set: { status: 'ready' } }
		);

		res.json({
			inserted: docs.length,
			message: `Computed New Product Ratio from Publication Time sheet`,
			processedData: [
				{ metric: 'new_product_ratio', value: ratio, unit: 'pct' },
			],
			details: detailRows,
		});
	} catch (e) {
		console.error('Publication Time processing failed:', e);
		res.status(500).json({
			error: 'Failed to process Publication Time sheet',
		});
	}
}

// Helper function to handle Origin of Seller sheet
async function handleOriginOfSellerSheet(dataset, rows, header, res) {
	try {
		const originIdx = header.findIndex((h) =>
			String(h).toLowerCase().includes('origin of seller')
		);
		const salesPropIdx = header.findIndex((h) =>
			String(h).toLowerCase().includes('sales proportion')
		);
		if (originIdx < 0 || salesPropIdx < 0) {
			return res.status(400).json({
				error: 'Origin of Seller or Sales Proportion column not found',
			});
		}

		const docs = [];
		const details = [];
		for (let r = 1; r < rows.length; r++) {
			const row = rows[r];
			const rawOrigin = row[originIdx];
			const rawSalesProp = row[salesPropIdx];
			if (rawOrigin == null || rawSalesProp == null) continue;
			const origin = String(rawOrigin).trim();
			let pct = rawSalesProp;
			if (typeof pct === 'string')
				pct = parseFloat(pct.replace(/[%\s,]/g, ''));
			if (typeof pct !== 'number' || Number.isNaN(pct)) continue;
			if (pct > 0 && pct <= 1) pct = pct * 100; // normalize if 0-1

			const code = origin
				.toLowerCase()
				.replace(/[^a-z]/g, '_')
				.replace(/_+/g, '_')
				.replace(/^_|_$/g, '');

			docs.push({
				datasetId: dataset._id,
				categoryId: dataset.categoryId,
				metric: `seller_origin_${code}`,
				bucket: 'overall',
				value: pct,
				unit: 'pct',
				sourceSheet: 'Origin of Seller',
			});
			details.push({ origin, salesProportion: pct });
		}

		if (!docs.length)
			return res
				.status(400)
				.json({ error: 'No valid rows in Origin of Seller sheet' });

		await ResearchSeries.deleteMany({
			datasetId: dataset._id,
			sourceSheet: 'Origin of Seller',
		});
		await ResearchSeries.insertMany(docs);
		await ResearchDataset.updateOne(
			{ _id: dataset._id },
			{ $set: { status: 'ready' } }
		);

		res.json({
			inserted: docs.length,
			message: 'Processed Origin of Seller sheet',
			processedData: docs.map((d) => ({
				metric: d.metric,
				value: d.value,
				unit: d.unit,
			})),
			details,
		});
	} catch (e) {
		console.error('Origin of Seller processing failed:', e);
		res.status(500).json({
			error: 'Failed to process Origin of Seller sheet',
		});
	}
}

// Helper function to handle Listing Concentration sheet
async function handleListingConcentrationSheet(dataset, rows, header, res) {
	try {
		const rankIdx = header.findIndex(
			(h) =>
				String(h).toLowerCase() === 'ranking' ||
				String(h).toLowerCase().includes('rank')
		);
		const salesPropIdx = header.findIndex((h) =>
			String(h).toLowerCase().includes('sales proportion')
		);
		if (rankIdx < 0 || salesPropIdx < 0) {
			return res.status(400).json({
				error: 'Ranking or Sales Proportion column not found',
			});
		}

		let totalTop10 = 0;
		const details = [];
		for (let r = 1; r < rows.length; r++) {
			const row = rows[r];
			const rawRank = row[rankIdx];
			const rawSalesProp = row[salesPropIdx];
			if (rawRank == null || rawSalesProp == null) continue;
			const rankNum = Number(
				String(rawRank)
					.toString()
					.replace(/[^0-9.-]/g, '')
			);
			if (!Number.isFinite(rankNum)) continue;
			let pct = rawSalesProp;
			if (typeof pct === 'string')
				pct = parseFloat(pct.replace(/[%\s,]/g, ''));
			if (!Number.isFinite(pct)) continue;
			if (pct > 0 && pct <= 1) pct = pct * 100; // normalize
			if (rankNum >= 1 && rankNum <= 10) {
				totalTop10 += pct;
				details.push({ ranking: rankNum, salesProportion: pct });
			}
		}

		const docs = [
			{
				datasetId: dataset._id,
				categoryId: dataset.categoryId,
				metric: 'listing_concentration',
				bucket: 'top10',
				value: totalTop10,
				unit: 'pct',
				sourceSheet: 'Listing Concentration',
			},
		];

		await ResearchSeries.deleteMany({
			datasetId: dataset._id,
			sourceSheet: 'Listing Concentration',
		});
		await ResearchSeries.insertMany(docs);
		await ResearchDataset.updateOne(
			{ _id: dataset._id },
			{ $set: { status: 'ready' } }
		);

		res.json({
			inserted: docs.length,
			message:
				'Computed Listing Concentration (Top 10 Sales Proportion Sum)',
			processedData: [
				{
					metric: 'listing_concentration',
					value: totalTop10,
					unit: 'pct',
				},
			],
			details: details.sort((a, b) => a.ranking - b.ranking),
		});
	} catch (e) {
		console.error('Listing Concentration processing failed:', e);
		res.status(500).json({
			error: 'Failed to process Listing Concentration sheet',
		});
	}
}

// Helper: compute dimensional tier and FBA fee from Avg.Weight(lb) and Avg.Volume(in^3)
async function handleMarketResearchWeightSheet(dataset, rows, header, res) {
	try {
		// Helper: normalize tier naming similar to frontend dashboard.js
		const normalizeTierName = (t) => {
			if (!t) return t;
			const s = String(t).toLowerCase();
			if (s.includes('small') && s.includes('standard'))
				return 'Small Standard';
			if (s.includes('large') && s.includes('standard'))
				return 'Large Standard';
			if (s.includes('oversize') || s.includes('over size'))
				return 'Oversize';
			return t;
		};
		// find headers by tolerant matching
		const findIdx = (pred) =>
			header.findIndex((h) => pred(String(h).toLowerCase()));
		const weightIdx = findIdx(
			(l) => l.includes('avg.weight') || l.includes('avg weight')
		);
		const volumeIdx = findIdx(
			(l) => l.includes('avg.volume') || l.includes('avg volume')
		);
		if (weightIdx < 0 || volumeIdx < 0) {
			return res.status(400).json({
				error: 'Avg.Weight(pounds) or Avg.Volume(in³) column not found',
			});
		}

		// parse first data row (sheet is usually summary)
		let avgWeightLb = null;
		let avgVolumeIn3 = null;
		for (let r = 1; r < rows.length; r++) {
			const w = rows[r][weightIdx];
			const v = rows[r][volumeIdx];
			if (w == null || v == null || w === '' || v === '') continue;
			const parseNum = (val) => {
				if (typeof val === 'number') return val;
				const s = String(val).replace(/[^0-9.\-]/g, '');
				const n = Number(s);
				return Number.isFinite(n) ? n : null;
			};
			avgWeightLb = parseNum(w);
			avgVolumeIn3 = parseNum(v);
			if (avgWeightLb != null && avgVolumeIn3 != null) break;
		}

		if (avgWeightLb == null || avgVolumeIn3 == null) {
			return res
				.status(400)
				.json({ error: 'No valid Avg.Weight/Avg.Volume values found' });
		}

		// Determine size tier using SizeTierRule by approximating a cube from volume
		// Example: Weight = 0.24 lb, Volume = 64.54 in³ -> ∛(64.54) ≈ 4 in → 4×4×4 in
		const side = Math.cbrt(Math.max(0, avgVolumeIn3));

		// Calculate dimensional weight (L × W × H / 139)
		const dimensionalWeight = (side * side * side) / 139;

		// Use the greater of actual weight or dimensional weight
		const shippingWeight = Math.max(avgWeightLb, dimensionalWeight);

		const dims = {
			longest: side,
			median: side,
			shortest: side,
			lengthGirth: side + 2 * (side + side), // L + 2*(W + H)
		};

		const allTiers = await SizeTierRule.find({}).lean();
		// normalize to inches/lb (rules store unitLength/unitWeight)
		const inFrom = (val, unit) => (unit === 'cm' ? val / 2.54 : val);
		const lbFrom = (val, unit) => (unit === 'oz' ? val / 16 : val);

		let matchedTier = null;
		for (const rule of allTiers) {
			const longestMaxIn =
				rule.longestMax != null
					? inFrom(rule.longestMax, rule.unitLength)
					: null;
			const medianMaxIn =
				rule.medianMax != null
					? inFrom(rule.medianMax, rule.unitLength)
					: null;
			const shortestMaxIn =
				rule.shortestMax != null
					? inFrom(rule.shortestMax, rule.unitLength)
					: null;
			const lengthGirthMaxIn =
				rule.lengthGirthMax != null
					? inFrom(rule.lengthGirthMax, rule.unitLength)
					: null;
			const shipWeightMaxLb =
				rule.shippingWeightMax != null
					? lbFrom(rule.shippingWeightMax, rule.unitWeight)
					: null;

			// Check dimensional constraints
			const fitsDims =
				(longestMaxIn == null || dims.longest <= longestMaxIn + 1e-6) &&
				(medianMaxIn == null || dims.median <= medianMaxIn + 1e-6) &&
				(shortestMaxIn == null ||
					dims.shortest <= shortestMaxIn + 1e-6) &&
				(lengthGirthMaxIn == null ||
					dims.lengthGirth <= lengthGirthMaxIn + 1e-6);

			// Check weight constraint using shipping weight (max of actual and dimensional)
			const fitsWeight =
				shipWeightMaxLb == null ||
				shippingWeight <= shipWeightMaxLb + 1e-6;

			if (fitsDims && fitsWeight) {
				matchedTier = rule.tier;
				break;
			}
		}

		if (!matchedTier) {
			return res.status(400).json({
				error: 'No size tier matches given Avg.Weight/Avg.Volume',
			});
		}

		// Compute FBA fee: find FbaFeeRule for matched tier, use shipping weight
		const normalizedTier = normalizeTierName(matchedTier);
		let feeRules = await FbaFeeRule.find({ tier: normalizedTier }).lean();
		if (!feeRules.length) {
			// Fallback: regex search by semantic bucket (small/large standard, oversize)
			const s = String(matchedTier).toLowerCase();
			let regex = null;
			if (s.includes('small') && s.includes('standard'))
				regex = /small\s*.*\s*standard/i;
			else if (s.includes('large') && s.includes('standard'))
				regex = /large\s*.*\s*standard/i;
			else if (s.includes('oversize') || s.includes('over size'))
				regex = /over\s*.*\s*size/i;
			if (regex) {
				feeRules = await FbaFeeRule.find({
					tier: { $regex: regex },
				}).lean();
			}
		}
		if (!feeRules.length) {
			return res.status(400).json({
				error: `No FBA fee rules found for tier ${matchedTier}`,
			});
		}

		// Convert weight to different units as needed
		const toUnit = (lb, unit) => {
			if (unit === 'oz') return lb * 16;
			if (unit === 'lb') return lb;
			return lb; // fallback
		};

		let feeUSD = null;
		let feeRulePicked = null;

		// Find matching weight band in FBA fee rules
		for (const rule of feeRules) {
			const w = toUnit(shippingWeight, rule.unit || 'oz');
			const min = rule.weightMin ?? 0;
			const max = rule.weightMax == null ? Infinity : rule.weightMax;

			if (w >= min && w <= max) {
				if (rule.feeUSD != null) {
					// Fixed fee
					feeUSD = rule.feeUSD;
				} else if (
					rule.baseUSD != null &&
					Array.isArray(rule.overageRules) &&
					rule.overageRules.length
				) {
					// Base fee + overage
					let total = rule.baseUSD;
					for (const over of rule.overageRules) {
						// Convert weight to overage rule units
						const overTo = (val, unit) => {
							if (unit === 'oz') return val * 16;
							if (unit === 'lb') return val;
							return val;
						};
						const current = overTo(
							shippingWeight,
							over.overThresholdUnit
						);
						if (current > over.overThresholdValue) {
							const overage = current - over.overThresholdValue;
							const steps = Math.ceil(overage / over.stepValue);
							total += steps * (over.stepFeeUSD || 0);
						}
					}
					feeUSD = total;
				}
				feeRulePicked = rule;
				break;
			}
		}

		if (feeUSD == null) {
			return res
				.status(400)
				.json({ error: 'No matching weight band in FBA fee rules' });
		}

		// Persist category-level constants for FBA fee and size tier estimate
		try {
			await ResearchCategory.updateOne(
				{ _id: dataset.categoryId },
				{
					$set: {
						fbaFeeUsd: feeUSD,
						sizeTierEstimate: normalizedTier,
						avgWeightLb,
						avgVolumeIn3,
						estimatedSideIn: side,
						estimatedDimensionalWeightLb: dimensionalWeight,
						estimatedShippingWeightLb: shippingWeight,
					},
				}
			);
		} catch (e) {
			// non-fatal; continue
		}

		// Also persist a referral fee default for this category if rules exist (best-effort)
		try {
			const catDoc = await ResearchCategory.findById(
				dataset.categoryId
			).lean();
			if (catDoc) {
				const refRules = await ReferralFeeRule.find({
					category: { $regex: new RegExp(catDoc.name, 'i') },
				}).lean();
				if (refRules && refRules.length) {
					// Prefer a rule covering entire price range if available
					let pick = refRules.find(
						(r) =>
							(r.priceMin == null || r.priceMin === 0) &&
							(r.priceMax == null || r.priceMax === 0)
					);
					if (!pick) {
						// Fallback: choose rule with widest range
						pick = [...refRules].sort((a, b) => {
							const ar =
								(a.priceMax ?? Infinity) - (a.priceMin ?? 0);
							const br =
								(b.priceMax ?? Infinity) - (b.priceMin ?? 0);
							return ar - br;
						})[0];
					}
					// Derive a default percent and min fee across rules
					const defaultPercent =
						pick?.feePercent ?? refRules[0].feePercent ?? null;
					const minFeeUSD = Math.max(
						0,
						...refRules.map((r) => Number(r.minFeeUSD || 0))
					);
					await ResearchCategory.updateOne(
						{ _id: dataset.categoryId },
						{
							$set: {
								referralFeePercentDefault: defaultPercent,
								referralMinFeeUsd: Number.isFinite(minFeeUSD)
									? minFeeUSD
									: 0,
							},
						}
					);
				}
			}
		} catch (e) {
			// non-fatal; continue
		}

		const docs = [
			{
				datasetId: dataset._id,
				categoryId: dataset.categoryId,
				metric: 'fba_fee',
				bucket: 'overall',
				value: feeUSD,
				unit: 'usd',
				sourceSheet: 'Market-research',
			},
		];

		await ResearchSeries.deleteMany({
			datasetId: dataset._id,
			sourceSheet: 'Market-research',
		});
		await ResearchSeries.insertMany(docs);
		await ResearchDataset.updateOne(
			{ _id: dataset._id },
			{ $set: { status: 'ready' } }
		);

		return res.json({
			inserted: 1,
			message: `Computed FBA Fee from Market-research: Weight ${avgWeightLb}lb, Volume ${avgVolumeIn3}in³ → ∛(${avgVolumeIn3}) ≈ ${side.toFixed(
				1
			)}in → Dimensional: ${dimensionalWeight.toFixed(
				2
			)}lb → Shipping: ${shippingWeight.toFixed(
				2
			)}lb → ${matchedTier} → $${feeUSD.toFixed(2)}`,
			processedData: [
				{
					metric: 'fba_fee',
					value: feeUSD,
					unit: 'usd',
					tier: matchedTier,
					avgWeightLb,
					avgVolumeIn3,
					estimatedSide: side,
					dimensionalWeight,
					shippingWeight,
				},
			],
		});
	} catch (e) {
		console.error('Market-research FBA fee processing failed:', e);
		return res
			.status(500)
			.json({ error: 'Failed to process Market-research sheet' });
	}
}

// Use memory storage for serverless environments (Vercel)
const {
	upload,
	processExcelFromBuffer,
} = require('../middleware/memoryUpload');

function readSheetAsArray(workbook, sheetName) {
	let ws = workbook.Sheets[sheetName];
	if (!ws) {
		const target = String(sheetName || '').toLowerCase();
		const alt = workbook.SheetNames.find(
			(n) =>
				String(n).toLowerCase() === target ||
				String(n).toLowerCase().includes(target)
		);
		if (alt) ws = workbook.Sheets[alt];
	}
	if (!ws) return [];
	let rows = XLSX.utils.sheet_to_json(ws, { header: 1, defval: null });
	// drop leading completely-empty rows to avoid false "Empty sheet"
	const notEmpty = (arr) =>
		Array.isArray(arr) && arr.some((c) => c !== null && c !== '');
	while (rows.length && !notEmpty(rows[0])) rows.shift();
	return rows;
}

// Create a silent response object to reuse handler functions without sending HTTP responses
function createSilentRes() {
	return {
		_status: 200,
		_payload: null,
		status(code) {
			this._status = code;
			return this;
		},
		json(payload) {
			this._payload = payload;
			return this;
		},
	};
}

// Fire-and-forget auto ingestion for standard sheets
async function autoIngestStandardSheets(dataset) {
	try {
		// Use workbook data from memory or file path
		let workbook;
		if (dataset.workbookData) {
			workbook = XLSX.read(dataset.workbookData, {
				type: 'buffer',
				cellDates: true,
			});
		} else if (dataset.storagePath) {
			workbook = XLSX.readFile(dataset.storagePath, { cellDates: true });
		} else {
			console.warn('No workbook data available for auto-ingest');
			return;
		}
		const STANDARD_SHEETS = [
			'Market Analysis',
			'Listing Concentration',
			'Fulfillment',
			'Origin of Seller',
			'Publication Time',
		];

		for (const stdName of STANDARD_SHEETS) {
			const rows = readSheetAsArray(workbook, stdName);
			if (!rows.length) continue;
			const header = rows[0].map((h) => String(h || '').trim());
			const silentRes = createSilentRes();
			try {
				if (stdName === 'Market Analysis') {
					await handleMarketAnalysisSheet(
						dataset,
						rows,
						header,
						silentRes
					);
				} else if (stdName === 'Listing Concentration') {
					await handleListingConcentrationSheet(
						dataset,
						rows,
						header,
						silentRes
					);
				} else if (stdName === 'Fulfillment') {
					await handleFulfillmentSheet(
						dataset,
						rows,
						header,
						silentRes
					);
				} else if (stdName === 'Origin of Seller') {
					await handleOriginOfSellerSheet(
						dataset,
						rows,
						header,
						silentRes
					);
				} else if (stdName === 'Publication Time') {
					await handlePublicationTimeSheet(
						dataset,
						rows,
						header,
						silentRes
					);
				}
			} catch (e) {
				// swallow errors per sheet to allow others to continue
			}
		}

		// Also attempt Market-research style sheet if present by fuzzy name
		const mrName = (workbook.SheetNames || []).find((n) =>
			String(n).toLowerCase().includes('market-research')
		);
		if (mrName) {
			const rows = readSheetAsArray(workbook, mrName);
			if (rows.length) {
				const header = rows[0].map((h) => String(h || '').trim());
				const silentRes = createSilentRes();
				try {
					await handleMarketResearchWeightSheet(
						dataset,
						rows,
						header,
						silentRes
					);
				} catch (e) {}
			}
		}
	} catch (e) {
		// ignore auto ingest errors
	}
}

// POST /api/research/upload
router.post(
	'/research/upload',
	upload.single('file'),
	processExcelFromBuffer,
	async (req, res) => {
		try {
			const { categoryName, categorySlug } = req.body;
			if (!req.file)
				return res.status(400).json({ error: 'Missing file' });

			// Ensure category exists
			let category = await ResearchCategory.findOne({
				slug: categorySlug,
			});
			if (!category) {
				category = await ResearchCategory.create({
					name: categoryName || categorySlug,
					slug: categorySlug,
				});
			}

			// Use the workbook from memory buffer
			const workbook = req.excelWorkbook;
			const sheets = workbook.SheetNames.map((name) => {
				const ws = workbook.Sheets[name];
				const json = XLSX.utils.sheet_to_json(ws, { header: 1 });
				const headers = Array.isArray(json[0])
					? json[0].map((h) => String(h || '').trim())
					: [];
				return {
					sheetName: name,
					columns: headers,
					rows: Math.max(0, json.length - 1),
				};
			});

			// Try to detect period (YYYY-MM) from filename, e.g., ...-202508-....xlsx
			const fname = req.file.originalname || '';
			let detectedMonth = null;
			const m1 = fname.match(/(\d{4})(\d{2})(?!\d)/); // YYYYMM
			if (m1) {
				detectedMonth = `${m1[1]}-${m1[2]}`;
			} else {
				const m2 = fname.match(/(\d{4})[-_](\d{2})(?!\d)/); // YYYY-MM or YYYY_MM
				if (m2) detectedMonth = `${m2[1]}-${m2[2]}`;
			}

			const dataset = await ResearchDataset.create({
				categoryId: category._id,
				originalFilename: req.file.originalname,
				storagePath: null, // No file storage in serverless
				workbookData: req.file.buffer, // Store file data in memory
				sheets,
				status: 'parsed',
				timeRange: detectedMonth
					? {
							from: detectedMonth,
							to: detectedMonth,
							bucket: 'month',
					  }
					: undefined,
			});

			// Kick off server-side auto ingestion for standard sheets (non-blocking)
			setImmediate(() => autoIngestStandardSheets(dataset));

			res.json({
				uploadId: dataset._id,
				sheets: sheets.map((s) => s.sheetName),
				period: detectedMonth || null,
			});
		} catch (e) {
			console.error('Upload failed', e);
			res.status(500).json({ error: 'Upload failed' });
		}
	}
);

// POST /api/research/select-sheet
router.post('/research/select-sheet', async (req, res) => {
	try {
		const { uploadId, sheetName } = req.body;
		const dataset = await ResearchDataset.findById(uploadId);
		if (!dataset)
			return res.status(404).json({ error: 'Dataset not found' });
		const sheet = dataset.sheets.find((s) => s.sheetName === sheetName);
		if (!sheet) return res.status(400).json({ error: 'Sheet not found' });
		return res.json({ datasetId: dataset._id, rows: sheet.rows });
	} catch (e) {
		console.error('Select sheet failed', e);
		res.status(500).json({ error: 'Select sheet failed' });
	}
});

// POST /api/research/ingest
// Body: { datasetId, sheetName, ingest? } for preview
// Body: { datasetId, sheetName, bucketColumn, bucketFormat, metricMappings: [{ metric, column, unit? }] } for ingestion
router.post('/research/ingest', async (req, res) => {
	try {
		const {
			datasetId,
			sheetName,
			bucketColumn,
			bucketFormat,
			metricMappings,
			ingest,
		} = req.body;

		if (!datasetId || !sheetName) {
			return res
				.status(400)
				.json({ error: 'datasetId and sheetName required' });
		}

		const dataset = await ResearchDataset.findById(datasetId);
		if (!dataset)
			return res.status(404).json({ error: 'Dataset not found' });

		// Use workbook data from memory or file path
		let workbook;
		if (dataset.workbookData) {
			workbook = XLSX.read(dataset.workbookData, {
				type: 'buffer',
				cellDates: true,
			});
		} else if (dataset.storagePath) {
			workbook = XLSX.readFile(dataset.storagePath, { cellDates: true });
		} else {
			return res
				.status(400)
				.json({ error: 'No workbook data available' });
		}
		const rows = readSheetAsArray(workbook, sheetName);
		if (!rows.length) return res.status(400).json({ error: 'Empty sheet' });

		const header = rows[0].map((h) => String(h || '').trim());

		// If just preview, return sheet info
		if (!ingest) {
			return res.json({
				sheetName,
				rows: rows.length - 1, // exclude header
				columns: header,
				defaultBucket: dataset.timeRange?.from || null,
			});
		}

		// Special handling for "Market Analysis" sheet
		if (sheetName === 'Market Analysis') {
			return handleMarketAnalysisSheet(dataset, rows, header, res);
		}

		// Special handling for "Fulfillment" sheet
		if (sheetName === 'Fulfillment') {
			return handleFulfillmentSheet(dataset, rows, header, res);
		}

		// Special handling for "Publication Time" sheet
		if (sheetName === 'Publication Time') {
			return handlePublicationTimeSheet(dataset, rows, header, res);
		}

		// Special handling for "Origin of Seller" sheet
		if (sheetName === 'Origin of Seller') {
			return handleOriginOfSellerSheet(dataset, rows, header, res);
		}

		// Special handling for "Listing Concentration" sheet
		if (sheetName === 'Listing Concentration') {
			return handleListingConcentrationSheet(dataset, rows, header, res);
		}

		// Special handling for any sheet name including "Market-research"
		if (String(sheetName).toLowerCase().includes('market-research')) {
			return handleMarketResearchWeightSheet(dataset, rows, header, res);
		}

		// Full ingestion logic
		if (!bucketColumn || !Array.isArray(metricMappings)) {
			return res.status(400).json({
				error: 'bucketColumn and metricMappings required for ingestion',
			});
		}

		const bIdx = header.findIndex(
			(h) => h.toLowerCase() === String(bucketColumn).toLowerCase()
		);
		if (bIdx < 0)
			return res.status(400).json({ error: 'Bucket column not found' });

		// Build column index map for metric mappings
		const mm = metricMappings
			.map((m) => ({
				metric: m.metric,
				unit: m.unit || 'units',
				idx: header.findIndex(
					(h) => h.toLowerCase() === String(m.column).toLowerCase()
				),
			}))
			.filter((m) => m.idx >= 0);
		if (!mm.length)
			return res.status(400).json({ error: 'No mapped columns found' });

		const docs = [];
		for (let r = 1; r < rows.length; r++) {
			const row = rows[r];
			let bucket;
			const rawBucket = row[bIdx];
			if (rawBucket == null || rawBucket === '') {
				// fall back to dataset's detected month when sheet has no time column
				bucket = dataset.timeRange?.from || null;
				if (!bucket) continue; // cannot infer
			} else {
				bucket = String(rawBucket).trim();
			}

			// Normalize bucket to YYYY-MM when possible
			if (
				bucket &&
				bucketFormat === 'YYYYMM' &&
				/^(\d{6})$/.test(bucket)
			) {
				bucket = `${bucket.slice(0, 4)}-${bucket.slice(4, 6)}`;
			} else if (
				bucketFormat === 'MM/YYYY' &&
				/^(\d{1,2})\/(\d{4})$/.test(bucket)
			) {
				const match = bucket.match(/^(\d{1,2})\/(\d{4})$/);
				const month = match[1].padStart(2, '0');
				const year = match[2];
				bucket = `${year}-${month}`;
			}

			for (const m of mm) {
				const valRaw = row[m.idx];
				if (valRaw == null || valRaw === '') continue;

				// Remove commas and $ signs, % to numeric
				let v = valRaw;
				if (typeof v === 'string') {
					const s = v.replace(/[$,%\s]/g, '').replace(/,/g, '');
					v = Number(s);
				}
				if (typeof v !== 'number' || Number.isNaN(v)) continue;

				docs.push({
					datasetId: dataset._id,
					categoryId: dataset.categoryId,
					metric: m.metric,
					bucket,
					value: v,
					unit: m.unit,
					sourceSheet: sheetName,
				});
			}
		}

		if (docs.length === 0)
			return res.status(400).json({ error: 'No data rows ingested' });

		await ResearchSeries.deleteMany({
			datasetId: dataset._id,
			sourceSheet: sheetName,
		});
		await ResearchSeries.insertMany(docs);
		await ResearchDataset.updateOne(
			{ _id: dataset._id },
			{ $set: { status: 'ready' } }
		);

		res.json({ inserted: docs.length });
	} catch (e) {
		console.error('Ingest failed', e);
		res.status(500).json({ error: 'Ingest failed' });
	}
});

// GET /api/research/metrics-summary?datasetId=...&metrics=sales_units,revenue&from=YYYY-MM&to=YYYY-MM
router.get('/research/metrics-summary', async (req, res) => {
	try {
		const { datasetId, metrics, from, to } = req.query;
		if (!datasetId)
			return res.status(400).json({ error: 'datasetId required' });
		const metricArr = (metrics || '')
			.split(',')
			.map((s) => s.trim())
			.filter(Boolean);
		const match = { datasetId };
		if (metricArr.length) match.metric = { $in: metricArr };
		if (from || to) {
			match.bucket = {};
			if (from) match.bucket.$gte = from;
			if (to) match.bucket.$lte = to;
		}
		const itemsRaw = await ResearchSeries.find({ ...match })
			.sort({ bucket: 1 })
			.lean();
		const ds = await ResearchDataset.findById(datasetId).lean();
		const defaultMonth = ds?.timeRange?.from || null;
		const coerceMonth = (b) => {
			const s = String(b || '').trim();
			if (/^\d{4}-\d{2}$/.test(s)) return s; // already YYYY-MM
			if (/^\d{6}$/.test(s)) return `${s.slice(0, 4)}-${s.slice(4, 6)}`; // YYYYMM
			const m = s.match(/(\d{4})[-_\/]?(\d{2})/);
			if (m) return `${m[1]}-${m[2]}`;
			return defaultMonth || s || null;
		};

		// Prefer sampleType=All for sales_units within the same month
		const items = [];
		if (metricArr.includes('sales_units')) {
			const byMonth = new Map();
			for (const it of itemsRaw) {
				const month = coerceMonth(it.bucket);
				if (!month) continue;
				if (it.metric !== 'sales_units') {
					items.push({ ...it, bucket: month });
					continue;
				}
				const key = month;
				const prev = byMonth.get(key);
				const isAll = /all/i.test(it.sampleType || '');
				if (!prev) byMonth.set(key, it);
				else if (/all/i.test(prev.sampleType || '') ? false : isAll)
					byMonth.set(key, it);
				else if (
					!/all/i.test(prev.sampleType || '') &&
					(Number(it.sampleSize) || 0) >
						(Number(prev.sampleSize) || 0)
				)
					byMonth.set(key, it);
			}
			for (const sel of byMonth.values())
				items.push({ ...sel, bucket: coerceMonth(sel.bucket) });
		} else {
			for (const it of itemsRaw)
				items.push({ ...it, bucket: coerceMonth(it.bucket) });
		}

		const timeSet = new Set(items.map((x) => x.bucket));
		let timeBuckets = Array.from(timeSet).filter(Boolean).sort();
		if (!timeBuckets.length && defaultMonth) timeBuckets = [defaultMonth];
		const seriesByMetric = {};
		for (const it of items) {
			if (!seriesByMetric[it.metric]) seriesByMetric[it.metric] = {};
			seriesByMetric[it.metric][it.bucket] = it.value;
		}

		// Fallback: if no data mapped for requested metrics, pick latest record per metric
		if (
			(!Object.keys(seriesByMetric).length || !timeBuckets.length) &&
			metricArr.length
		) {
			for (const m of metricArr) {
				const latest = await ResearchSeries.findOne({
					datasetId,
					metric: m,
				})
					.sort({ bucket: -1, createdAt: -1 })
					.lean();
				if (!latest) continue;
				const b = coerceMonth(latest.bucket);
				if (!seriesByMetric[m]) seriesByMetric[m] = {};
				seriesByMetric[m][b] = latest.value;
				if (!timeBuckets.includes(b)) timeBuckets.push(b);
			}
			timeBuckets = timeBuckets.filter(Boolean).sort();
		}
		res.json({ timeBuckets, seriesByMetric });
	} catch (e) {
		console.error('Summary failed', e);
		res.status(500).json({ error: 'Summary failed' });
	}
});

// POST /api/research/compute-cogs
// Body: { datasetId, avgPricePctAds=20, targetProfitPct=20 }
router.post('/research/compute-cogs', async (req, res) => {
	try {
		const {
			datasetId,
			avgPricePctAds = 20,
			targetProfitPct = 20,
		} = req.body;
		if (!datasetId)
			return res.status(400).json({ error: 'datasetId required' });

		const ds = await ResearchDataset.findById(datasetId).lean();
		if (!ds) return res.status(404).json({ error: 'Dataset not found' });

		// Get latest avg_price from Market Analysis (use max bucket)
		const latestAvgPrice = await ResearchSeries.find({
			datasetId,
			metric: 'avg_price',
			sourceSheet: 'Market Analysis',
		})
			.sort({ bucket: -1 })
			.limit(1)
			.lean();

		if (!latestAvgPrice.length)
			return res.status(400).json({ error: 'avg_price not found' });

		const avgPrice = Number(latestAvgPrice[0].value) || 0;

		// Get latest referral_fee (if any)
		const latestReferral = await ResearchSeries.find({
			datasetId,
			metric: 'referral_fee',
		})
			.sort({ bucket: -1 })
			.limit(1)
			.lean();
		const referralFee = latestReferral.length
			? Number(latestReferral[0].value) || 0
			: 0;

		// Get latest fba_fee (overall)
		const latestFba = await ResearchSeries.find({
			datasetId,
			metric: 'fba_fee',
		})
			.sort({ createdAt: -1 })
			.limit(1)
			.lean();
		const fbaFee = latestFba.length ? Number(latestFba[0].value) || 0 : 0;

		// Compute components
		const ads = (avgPricePctAds / 100) * avgPrice;
		const profitTarget = (targetProfitPct / 100) * avgPrice;
		const fee = referralFee + fbaFee;
		const cogsCap = avgPrice - (ads + fee + profitTarget);

		// Save as metric cogs_cap (bucket: overall)
		await ResearchSeries.updateOne(
			{
				datasetId,
				metric: 'cogs_cap',
				bucket: 'overall',
			},
			{
				$set: {
					datasetId,
					categoryId: ds.categoryId,
					metric: 'cogs_cap',
					bucket: 'overall',
					value: cogsCap,
					unit: 'usd',
					sourceSheet: 'Derived',
				},
			},
			{ upsert: true }
		);

		return res.json({
			message: 'Computed COGS cap',
			processedData: [
				{
					metric: 'cogs_cap',
					value: cogsCap,
					unit: 'usd',
				},
			],
			details: {
				avgPrice,
				ads,
				referralFee,
				fbaFee,
				profitTarget,
			},
		});
	} catch (e) {
		console.error('Compute COGS failed', e);
		return res.status(500).json({ error: 'Failed to compute COGS cap' });
	}
});

// GET /api/research/categories
router.get('/research/categories', async (req, res) => {
	try {
		const items = await ResearchCategory.find({}).sort({ name: 1 }).lean();
		res.json(items);
	} catch (e) {
		res.status(500).json({ error: 'Failed to fetch categories' });
	}
});

// GET /api/research/category/:id - Get detailed category information with all metrics
router.get('/research/category/:id', async (req, res) => {
	try {
		const categoryId = req.params.id;

		// Get category info
		const category = await ResearchCategory.findById(categoryId).lean();
		if (!category) {
			return res.status(404).json({ error: 'Category not found' });
		}

		// Get all datasets for this category
		const datasets = await ResearchDataset.find({ categoryId })
			.sort({ createdAt: -1 })
			.lean();

		// Get all metrics for this category
		const allMetrics = await ResearchSeries.find({ categoryId })
			.sort({ createdAt: -1 })
			.lean();

		// Group metrics by dataset and metric type
		const metricsByDataset = {};
		const metricsSummary = {};

		for (const metric of allMetrics) {
			const datasetId = metric.datasetId.toString();
			const metricKey = metric.metric;

			// Initialize dataset if not exists
			if (!metricsByDataset[datasetId]) {
				metricsByDataset[datasetId] = {};
			}

			// Initialize metric if not exists
			if (!metricsByDataset[datasetId][metricKey]) {
				metricsByDataset[datasetId][metricKey] = [];
			}

			metricsByDataset[datasetId][metricKey].push({
				bucket: metric.bucket,
				value: metric.value,
				unit: metric.unit,
				sourceSheet: metric.sourceSheet,
				sampleSize: metric.sampleSize,
				sampleType: metric.sampleType,
				createdAt: metric.createdAt,
			});

			// Build summary (latest value for each metric)
			if (!metricsSummary[metricKey]) {
				metricsSummary[metricKey] = {
					latestValue: metric.value,
					latestBucket: metric.bucket,
					unit: metric.unit,
					totalRecords: 0,
					datasets: new Set(),
				};
			}

			metricsSummary[metricKey].totalRecords++;
			metricsSummary[metricKey].datasets.add(datasetId);

			// Update latest if this record is newer
			if (
				new Date(metric.createdAt) >
				new Date(metricsSummary[metricKey].latestCreatedAt || 0)
			) {
				metricsSummary[metricKey].latestValue = metric.value;
				metricsSummary[metricKey].latestBucket = metric.bucket;
				metricsSummary[metricKey].latestCreatedAt = metric.createdAt;
			}
		}

		// Convert datasets Set to Array and add dataset info
		for (const [metricKey, summary] of Object.entries(metricsSummary)) {
			summary.datasets = Array.from(summary.datasets).map((datasetId) => {
				const dataset = datasets.find(
					(d) => d._id.toString() === datasetId
				);
				return {
					id: datasetId,
					filename: dataset?.originalFilename,
					timeRange: dataset?.timeRange,
					createdAt: dataset?.createdAt,
				};
			});
		}

		// Get time buckets across all datasets (months only) and sort chronologically
		const allBucketsRaw = [...new Set(allMetrics.map((m) => m.bucket))];
		const allBuckets = allBucketsRaw
			.filter((b) => /^\d{4}-\d{2}$/.test(String(b)))
			.sort((a, b) => a.localeCompare(b));

		// Build time series data for each metric across all months
		const timeSeriesData = {};
		for (const [metricKey, summary] of Object.entries(metricsSummary)) {
			timeSeriesData[metricKey] = {
				metric: metricKey,
				unit: summary.unit,
				timeSeries: [],
			};

			// Group metrics by bucket for this metric
			const metricByBucket = {};
			for (const metric of allMetrics) {
				if (metric.metric === metricKey) {
					if (!metricByBucket[metric.bucket]) {
						metricByBucket[metric.bucket] = [];
					}
					metricByBucket[metric.bucket].push(metric);
				}
			}

			// For each time bucket, get the best value (prioritize by sampleType and sampleSize)
			for (const bucket of allBuckets) {
				if (metricByBucket[bucket]) {
					const bucketMetrics = metricByBucket[bucket];

					// For sales_units, prioritize "All" sampleType, then largest sampleSize
					if (metricKey === 'sales_units') {
						const allSample = bucketMetrics.find(
							(m) => m.sampleType === 'All'
						);
						if (allSample) {
							timeSeriesData[metricKey].timeSeries.push({
								bucket: bucket,
								value: allSample.value,
								sampleType: allSample.sampleType,
								sampleSize: allSample.sampleSize,
								createdAt: allSample.createdAt,
							});
						} else {
							// Find the one with largest sampleSize
							const best = bucketMetrics.reduce((prev, current) =>
								prev.sampleSize > current.sampleSize
									? prev
									: current
							);
							timeSeriesData[metricKey].timeSeries.push({
								bucket: bucket,
								value: best.value,
								sampleType: best.sampleType,
								sampleSize: best.sampleSize,
								createdAt: best.createdAt,
							});
						}
					} else {
						// For other metrics, just take the first one (they should be the same)
						const metric = bucketMetrics[0];
						timeSeriesData[metricKey].timeSeries.push({
							bucket: bucket,
							value: metric.value,
							sampleType: metric.sampleType,
							sampleSize: metric.sampleSize,
							createdAt: metric.createdAt,
						});
					}
				}
			}
		}

		// Build comprehensive response
		const response = {
			category: {
				id: category._id,
				name: category.name,
				slug: category.slug,
				createdAt: category.createdAt,
				updatedAt: category.updatedAt,
				// Expose persisted fee/tier constants if available
				fbaFeeUsd: category.fbaFeeUsd ?? null,
				sizeTierEstimate: category.sizeTierEstimate ?? null,
				avgWeightLb: category.avgWeightLb ?? null,
				avgVolumeIn3: category.avgVolumeIn3 ?? null,
				estimatedSideIn: category.estimatedSideIn ?? null,
				estimatedDimensionalWeightLb:
					category.estimatedDimensionalWeightLb ?? null,
				estimatedShippingWeightLb:
					category.estimatedShippingWeightLb ?? null,
				referralFeePercentDefault:
					category.referralFeePercentDefault ?? null,
				referralMinFeeUsd: category.referralMinFeeUsd ?? null,
			},
			datasets: datasets.map((dataset) => ({
				id: dataset._id,
				filename: dataset.originalFilename,
				timeRange: dataset.timeRange,
				status: dataset.status,
				sheetCount: dataset.sheetCount,
				createdAt: dataset.createdAt,
				metrics: metricsByDataset[dataset._id.toString()] || {},
			})),
			metricsSummary: Object.entries(metricsSummary).map(
				([metric, data]) => ({
					metric,
					latestValue: data.latestValue,
					latestBucket: data.latestBucket,
					unit: data.unit,
					totalRecords: data.totalRecords,
					datasets: data.datasets,
				})
			),
			timeSeriesData: Object.values(timeSeriesData),
			timeBuckets: allBuckets,
			statistics: {
				totalDatasets: datasets.length,
				totalMetrics: Object.keys(metricsSummary).length,
				totalRecords: allMetrics.length,
				timeRange:
					allBuckets.length > 0
						? {
								from: allBuckets[0],
								to: allBuckets[allBuckets.length - 1],
						  }
						: null,
				monthsCovered: allBuckets.filter((b) => /^\d{4}-\d{2}$/.test(b))
					.length,
			},
		};

		res.json(response);
	} catch (e) {
		console.error('Category detail fetch failed', e);
		res.status(500).json({ error: 'Category detail fetch failed' });
	}
});

// POST /api/research/migrate-buckets - Fix invalid bucket values
router.post('/research/migrate-buckets', async (req, res) => {
	try {
		const { categoryId } = req.body;

		// Find all series with invalid bucket format
		const invalidBuckets = await ResearchSeries.find({
			categoryId: categoryId,
			bucket: { $regex: /^\d{3,4}$/ }, // Match 3-4 digit numbers like "2161", "950", "826"
		}).lean();

		if (invalidBuckets.length === 0) {
			return res.json({
				message: 'No invalid buckets found',
				updated: 0,
			});
		}

		// Get dataset info to extract correct timeRange
		const datasetIds = [...new Set(invalidBuckets.map((s) => s.datasetId))];
		const datasets = await ResearchDataset.find({
			_id: { $in: datasetIds },
		}).lean();

		const datasetMap = {};
		datasets.forEach((d) => {
			datasetMap[d._id.toString()] = d.timeRange?.from || 'overall';
		});

		// Update invalid buckets
		let updated = 0;
		for (const series of invalidBuckets) {
			const correctBucket =
				datasetMap[series.datasetId.toString()] || 'overall';

			await ResearchSeries.updateOne(
				{ _id: series._id },
				{ $set: { bucket: correctBucket } }
			);
			updated++;
		}

		res.json({
			message: `Updated ${updated} records with invalid buckets`,
			updated,
			invalidBuckets: invalidBuckets.length,
		});
	} catch (e) {
		console.error('Bucket migration failed', e);
		res.status(500).json({ error: 'Bucket migration failed' });
	}
});

// POST /api/research/categories (create)
router.post('/research/categories', async (req, res) => {
	try {
		const { name, description } = req.body;
		if (!name) return res.status(400).json({ error: 'name required' });
		console.log('Creating category:', { name, description });
		const doc = await ResearchCategory.create({ name, description });
		console.log('Category created successfully:', doc);
		res.status(201).json(doc);
	} catch (e) {
		console.error('Failed to create category:', e);
		res.status(500).json({
			error: 'Failed to create category',
			details: e.message,
		});
	}
});

// GET /api/research/select-sheet?datasetId=...
router.get('/research/select-sheet', async (req, res) => {
	try {
		const { datasetId } = req.query;
		if (!datasetId)
			return res.status(400).json({ error: 'datasetId required' });

		const dataset = await ResearchDataset.findById(datasetId).lean();
		if (!dataset)
			return res.status(404).json({ error: 'Dataset not found' });

		const sheets = (dataset.sheets || []).map((s) => ({
			name: s.sheetName,
			rows: s.rows,
			columns: s.columns || [],
		}));

		res.json({ sheets });
	} catch (e) {
		console.error('Failed to fetch sheets', e);
		res.status(500).json({ error: 'Failed to fetch sheets' });
	}
});

module.exports = router;

// GET /api/research/datasets?categorySlug=...
router.get('/research/datasets', async (req, res) => {
	try {
		const { categorySlug } = req.query;
		if (!categorySlug)
			return res.status(400).json({ error: 'categorySlug required' });
		const cat = await ResearchCategory.findOne({
			slug: categorySlug,
		}).lean();
		if (!cat) return res.status(404).json({ error: 'Category not found' });
		const items = await ResearchDataset.find({ categoryId: cat._id })
			.sort({ createdAt: -1 })
			.lean();
		const mapped = items.map((d) => ({
			id: d._id,
			originalFilename: d.originalFilename,
			status: d.status,
			createdAt: d.createdAt,
			sheetCount: (d.sheets || []).length,
			sheets: (d.sheets || []).map((s) => ({
				name: s.sheetName,
				rows: s.rows,
			})),
		}));
		res.json({
			category: { id: cat._id, name: cat.name, slug: cat.slug },
			datasets: mapped,
		});
	} catch (e) {
		res.status(500).json({ error: 'Failed to fetch datasets' });
	}
});

// DELETE /api/research/datasets/:id
router.delete('/research/datasets/:id', async (req, res) => {
	try {
		const { id } = req.params;
		const ds = await ResearchDataset.findById(id);
		if (!ds) return res.status(404).json({ error: 'Dataset not found' });

		// delete series belonging to this dataset
		await ResearchSeries.deleteMany({ datasetId: ds._id });

		// delete stored file if exists
		if (ds.storagePath) {
			try {
				fs.unlinkSync(ds.storagePath);
			} catch (e) {}
		}

		await ResearchDataset.deleteOne({ _id: ds._id });
		return res.json({ deleted: true });
	} catch (e) {
		console.error('Delete dataset failed', e);
		return res.status(500).json({ error: 'Failed to delete dataset' });
	}
});

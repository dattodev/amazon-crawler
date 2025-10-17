let displayMode = 'grid'; // or 'table'
let currentTab = 'regular'; // 'regular' | 'bestsellers' | 'feerule' | 'fbafee'
let imageOnlyCols = 0; // 0=off, 4/5/6 columns when image-only

// Cache for referral fee rules
let cacheReferralFeeRules = [];

// Fetch referral fee rules
async function fetchReferralFeeRules() {
	try {
		const response = await fetch(
			`${window.API_BASE_URL || ''}/api/fee-rules`
		);
		if (!response.ok) {
			throw new Error(`HTTP error! status: ${response.status}`);
		}
		const responseData = await response.json();
		// console.log('Raw API response:', responseData);
		// console.log('Response type:', typeof responseData);
		// console.log('Is array:', Array.isArray(responseData));
		// console.log('Length:', responseData?.length);

		// Ensure it's an array
		if (Array.isArray(responseData)) {
			cacheReferralFeeRules = responseData;
		} else {
			console.warn('API response is not an array, converting...');
			cacheReferralFeeRules = [];
		}

		// console.log('Final cacheReferralFeeRules:', cacheReferralFeeRules);
		// console.log('Final length:', cacheReferralFeeRules.length);
	} catch (error) {
		console.error('Error fetching referral fee rules:', error);
		cacheReferralFeeRules = [];
	}
}

// Calculate referral fee based on category and price
function calculateReferralFee(category, price) {
	if (!Array.isArray(cacheReferralFeeRules) || !category || !price) {
		return null;
	}

	// Find matching rules for this category
	const matchingRules = cacheReferralFeeRules.filter((rule) => {
		// Check category match (case insensitive)
		const ruleCategory = rule.category?.toLowerCase() || '';
		const productCategory = category?.toLowerCase() || '';

		// Check if rule category is contained in product category or vice versa
		const categoryMatch =
			ruleCategory.includes(productCategory) ||
			productCategory.includes(ruleCategory) ||
			ruleCategory === productCategory;

		if (!categoryMatch) return false;

		// Check price range
		const priceMin = rule.priceMin || 0;
		const priceMax =
			rule.priceMax !== 0 && rule.priceMax !== null
				? rule.priceMax
				: Infinity;

		const priceMatch = price >= priceMin && price <= priceMax;

		console.log(
			`Rule check - Category: "${ruleCategory}" vs "${productCategory}", Price: ${price} in [${priceMin}, ${priceMax}], Match: ${
				categoryMatch && priceMatch
			}`
		);

		return priceMatch;
	});

	if (matchingRules.length === 0) {
		return null;
	}

	// Sort rules by price range (prefer more specific ranges)
	matchingRules.sort((a, b) => {
		const aRange = (a.priceMax || Infinity) - (a.priceMin || 0);
		const bRange = (b.priceMax || Infinity) - (b.priceMin || 0);
		return aRange - bRange;
	});

	// Calculate fee based on Apply_To type
	let totalFee = 0;
	console.log(matchingRules);

	for (const rule of matchingRules) {
		const applyTo = rule.applyTo?.toLowerCase() || 'total';
		if (applyTo === 'total') {
			// TH1: Apply_To = Total
			// Fee = Price x Referral_Rate
			const fee = price * rule.feePercent;
			totalFee += fee;
		} else if (applyTo === 'portion') {
			// TH2: Apply_To = Portion
			// Calculate fee for the portion of price that falls within this rule's range
			const rulePriceMin = rule.priceMin || 0;
			const rulePriceMax = rule.priceMax || Infinity;

			const portionStart = Math.max(rulePriceMin, 0);
			const portionEnd = Math.min(price, rulePriceMax);
			const portionAmount = Math.max(0, portionEnd - portionStart);

			if (portionAmount > 0) {
				const fee = portionAmount * rule.feePercent;
				totalFee += fee;
			}
		}
	}

	// Apply minimum fee if specified
	const minFee = Math.max(
		...matchingRules.map((rule) => rule.minFeeUSD || 0)
	);
	console.log(`Total fee before min: ${totalFee}, Min fee: ${minFee}`);

	if (minFee > 0) {
		totalFee = Math.max(totalFee, minFee);
		console.log(`Final fee after min: ${totalFee}`);
	}

	return totalFee > 0 ? totalFee : null;
}

function getFilterParams() {
	let search = '';
	const minPriceInput = document.getElementById('minPrice');
	const maxPriceInput = document.getElementById('maxPrice');
	const minPrice =
		minPriceInput && minPriceInput.value !== ''
			? parseFloat(minPriceInput.value)
			: null;
	const maxPrice =
		maxPriceInput && maxPriceInput.value !== ''
			? parseFloat(maxPriceInput.value)
			: null;
	const startDateInput = document.getElementById('startDate');
	const endDateInput = document.getElementById('endDate');
	const startDate = startDateInput ? startDateInput.value : null;
	const endDate = endDateInput ? endDateInput.value : null;
	const boughtInLast30DaysInput =
		document.getElementById('boughtInPastMonth');
	const boughtInLast30Days = !!(
		boughtInLast30DaysInput && boughtInLast30DaysInput.checked
	);
	const searchInput = document.getElementById('searchInput');
	if (searchInput) search = searchInput.value.trim();

	const params = [];
	if (search) params.push(`search=${encodeURIComponent(search)}`);
	if (minPrice !== null) params.push(`minPrice=${minPrice}`);
	if (maxPrice !== null) params.push(`maxPrice=${maxPrice}`);
	if (startDate) params.push(`startDate=${startDate}`);
	if (endDate) params.push(`endDate=${endDate}`);
	if (boughtInLast30Days) params.push('boughtInLast30Days=true');

	return params;
}

async function fetchProducts() {
	try {
		const params = getFilterParams();
		let url = `${window.API_BASE_URL || ''}/api/products`;

		// Add source filter based on current tab
		if (currentTab === 'bestsellers') {
			params.push('source=best-sellers,new-releases');
		} else if (currentTab === 'regular') {
			// For regular tab, exclude best-sellers and new-releases
			params.push(
				'source=direct,marketplace,best-selling,new-arrivals,unknown'
			);
		} else {
			// Fee rule tab doesn't fetch products
			return [];
		}

		if (params.length) url += '?' + params.join('&');

		const res = await fetch(url);
		if (!res.ok) {
			throw new Error(`HTTP error! status: ${res.status}`);
		}
		return await res.json();
	} catch (error) {
		console.error('Error fetching products:', error);
		showError('Failed to load products. Please try again.');
		return [];
	}
}

async function fetchProductsCount() {
	try {
		const params = getFilterParams();
		let url = `${window.API_BASE_URL || ''}/api/products/count`;

		// Add source filter based on current tab
		if (currentTab === 'bestsellers') {
			params.push('source=best-sellers,new-releases');
		} else if (currentTab === 'regular') {
			// For regular tab, exclude best-sellers and new-releases
			params.push(
				'source=direct,marketplace,best-selling,new-arrivals,unknown'
			);
		} else {
			return 0;
		}

		if (params.length) url += '?' + params.join('&');

		const res = await fetch(url);
		if (!res.ok) {
			throw new Error(`HTTP error! status: ${res.status}`);
		}
		const data = await res.json();
		return data.count || 0;
	} catch (error) {
		console.error('Error fetching products count:', error);
		return 0;
	}
}
async function fetchSoldByList() {
	try {
		const params = [];
		// Add source filter based on current tab
		if (currentTab === 'bestsellers') {
			params.push('source=best-sellers,new-releases');
		} else {
			// For regular tab, exclude best-sellers and new-releases
			params.push(
				'source=direct,marketplace,best-selling,new-arrivals,unknown'
			);
		}

		let url = `${window.API_BASE_URL || ''}/api/products/soldby-list`;
		if (params.length) url += '?' + params.join('&');

		const res = await fetch(url);
		if (!res.ok) {
			throw new Error(`HTTP error! status: ${res.status}`);
		}
		return await res.json();
	} catch (error) {
		console.error('Error fetching sold by list:', error);
		return [];
	}
}

async function fetchCategoryList() {
	try {
		const params = [];
		// Add source filter based on current tab
		if (currentTab === 'bestsellers') {
			params.push('source=best-sellers,new-releases');
		} else {
			// For regular tab, exclude best-sellers and new-releases
			params.push(
				'source=direct,marketplace,best-selling,new-arrivals,unknown'
			);
		}

		let url = `${window.API_BASE_URL || ''}/api/products/category-list`;
		if (params.length) url += '?' + params.join('&');

		const res = await fetch(url);
		if (!res.ok) {
			throw new Error(`HTTP error! status: ${res.status}`);
		}
		return await res.json();
	} catch (error) {
		console.error('Error fetching category list:', error);
		return [];
	}
}

// Research categories (to map category name -> id for insight link)
async function fetchResearchCategories() {
	try {
		const res = await fetch(
			`${window.API_BASE_URL || ''}/api/research/categories`
		);
		if (!res.ok) throw new Error(`HTTP ${res.status}`);
		return await res.json();
	} catch (e) {
		console.error('Error fetching research categories:', e);
		return [];
	}
}

function slugifyCategoryName(name) {
	return String(name || '')
		.toLowerCase()
		.trim()
		.replace(/[_\s]+/g, '-')
		.replace(/[^a-z0-9-]/g, '-')
		.replace(/-+/g, '-')
		.replace(/^-|-$/g, '');
}

function getResearchCategoryIdFromName(name) {
	if (!Array.isArray(window.researchCategories)) return null;
	const slug = slugifyCategoryName(name);
	const found = window.researchCategories.find(
		(c) =>
			c.slug === slug ||
			(c.name &&
				c.name.toLowerCase() === String(name || '').toLowerCase())
	);
	return found ? found._id || found.id : null;
}

async function fetchSourceList() {
	try {
		const params = [];
		// Add source filter based on current tab
		if (currentTab === 'bestsellers') {
			params.push('source=best-sellers,new-releases');
		} else {
			// For regular tab, exclude best-sellers and new-releases
			params.push(
				'source=direct,marketplace,best-selling,new-arrivals,unknown'
			);
		}

		let url = `${window.API_BASE_URL || ''}/api/products/source-list`;
		if (params.length) url += '?' + params.join('&');

		const res = await fetch(url);
		if (!res.ok) {
			console.warn(
				`Source list API returned ${res.status}, using empty array`
			);
			return [];
		}
		const data = await res.json();
		return Array.isArray(data) ? data : [];
	} catch (error) {
		console.error('Error fetching source list:', error);
		return [];
	}
}
function renderSidebar(sellers, categories, sources) {
	const sellerDiv = document.getElementById('sellerFilters');
	const categoryDiv = document.getElementById('categoryFilters');
	const sourceDiv = document.getElementById('sourceFilters');
	if (!sellerDiv || !categoryDiv || !sourceDiv) return;

	// Render sellers with search and pagination
	let sellerHtml = `
		<div class="filter-search">
			<input type="text" id="sellerSearch" placeholder="Search seller.." class="filter-search-input">
		</div>
		<div class="filter-list" id="sellerList">
			<label class="filter-option">
				<input type="checkbox" name="seller" value="all" checked>
				<span>All</span>
				<a href="#" class="clear-link" onclick="clearSellerFilters()">Clear</a>
			</label>
	`;

	const visibleSellers = sellers.slice(0, 5);
	visibleSellers.forEach((seller) => {
		sellerHtml += `<label class="filter-option"><input type="checkbox" name="seller" value="${seller}"><span>${seller}</span><span class="count">0</span></label>`;
	});

	if (sellers.length > 5) {
		sellerHtml += `<div class="show-more" onclick="toggleShowMore('seller')">Show more (${
			sellers.length - 5
		})</div>`;
	}

	sellerHtml += `</div>`;
	sellerDiv.innerHTML = sellerHtml;

	// Render categories with search and pagination
	let categoryHtml = `
		<div class="filter-search">
			<input type="text" id="categorySearch" placeholder="Search category.." class="filter-search-input">
		</div>
		<div class="filter-list" id="categoryList">
			<label class="filter-option">
				<input type="checkbox" name="category" value="all" checked>
				<span>All</span>
				<a href="#" class="clear-link" onclick="clearCategoryFilters()">Clear</a>
			</label>
	`;

	const visibleCategories = categories.slice(0, 5);
	visibleCategories.forEach((category) => {
		const id = getResearchCategoryIdFromName(category);
		const detailLink = id
			? `<a href="/dashboard/insight.html?id=${id}" target="_blank" class="detail-link" style="margin-left:8px;font-size:12px;color:#2278d0;text-decoration:none;">chi tiết</a>`
			: '';
		categoryHtml += `<label class="filter-option"><input type="checkbox" name="category" value="${category}"><span>${
			category.length > 10 ? category.substring(0, 10) + '...' : category
		}</span><span class="count">0</span>${detailLink}</label>`;
	});

	if (categories.length > 5) {
		categoryHtml += `<div class="show-more" onclick="toggleShowMore('category')">Show more (${
			categories.length - 5
		})</div>`;
	}

	categoryHtml += `</div>`;
	categoryDiv.innerHTML = categoryHtml;

	// Render sources (keep simple for now)
	let sourceHtml = `<label class="filter-option"><input type="checkbox" name="source" value="all" checked>All</label>`;
	sources.forEach((source) => {
		const label =
			source.charAt(0).toUpperCase() + source.slice(1).replace('-', ' ');
		sourceHtml += `<label class="filter-option"><input type="checkbox" name="source" value="${source}"><span>${label}</span></label>`;
	});
	sourceDiv.innerHTML = sourceHtml;

	// Add event listeners
	document.querySelectorAll('input[name="seller"]').forEach((input) => {
		input.addEventListener(
			'change',
			async () => await onSellerFilterChange.call(input)
		);
	});
	document.querySelectorAll('input[name="category"]').forEach((input) => {
		input.addEventListener(
			'change',
			async () => await onCategoryFilterChange.call(input)
		);
	});
	document.querySelectorAll('input[name="source"]').forEach((input) => {
		input.addEventListener(
			'change',
			async () => await onSourceFilterChange.call(input)
		);
	});

	// Add search functionality
	setupFilterSearch('sellerSearch', 'seller');
	setupFilterSearch('categorySearch', 'category');
}

// Helper functions for sidebar
function getSellerCounts() {
	const counts = {};
	if (window.products) {
		window.products.forEach((product) => {
			if (product.soldBy) {
				counts[product.soldBy] = (counts[product.soldBy] || 0) + 1;
			}
		});
	}
	return counts;
}

function getCategoryCounts() {
	const counts = {};
	if (window.products) {
		window.products.forEach((product) => {
			if (product.category) {
				counts[product.category] = (counts[product.category] || 0) + 1;
			}
		});
	}
	return counts;
}

function setupFilterSearch(searchInputId, filterType) {
	const searchInput = document.getElementById(searchInputId);
	if (searchInput) {
		searchInput.addEventListener('input', (e) => {
			const searchTerm = e.target.value.toLowerCase();
			const filterList = document.getElementById(filterType + 'List');
			const options = filterList.querySelectorAll('.filter-option');
			const showMoreElement = filterList.querySelector('.show-more');

			// Hide/show options based on search
			options.forEach((option) => {
				const text = option.textContent.toLowerCase();
				if (text.includes(searchTerm)) {
					option.style.display = 'flex';
				} else {
					option.style.display = 'none';
				}
			});

			// Hide/show "Show more/less" button based on search
			if (showMoreElement) {
				if (searchTerm.trim() === '') {
					// Show button when no search term
					showMoreElement.style.display = 'block';
				} else {
					// Hide button when searching
					showMoreElement.style.display = 'none';
				}
			}
		});
	}
}

function toggleShowMore(type) {
	const listElement = document.getElementById(type + 'List');
	if (!listElement) return;

	// Get all items for this type
	let allItems = [];
	if (type === 'seller') {
		allItems = window.allSellers || [];
	} else if (type === 'category') {
		allItems = window.allCategories || [];
	}

	if (allItems.length === 0) return;

	// Check if currently showing limited items by checking button text
	const showMoreElement = listElement.querySelector('.show-more');
	if (!showMoreElement) return;

	const buttonText = showMoreElement.textContent.trim();
	const isShowingLimited = buttonText.includes('Show more');

	if (isShowingLimited) {
		// Show all items
		showAllItems(type, allItems);
	} else {
		// Show limited items
		showLimitedItems(type, allItems);
	}
}

function showAllItems(type, allItems) {
	const listElement = document.getElementById(type + 'List');
	if (!listElement) return;

	// Preserve selected filters
	const selectedValues = preserveSelectedFilters(type);

	// Get counts
	const counts = type === 'seller' ? getSellerCounts() : getCategoryCounts();

	// Rebuild HTML with all items
	let html = `
		<label class="filter-option">
			<input type="checkbox" name="${type}" value="all" ${
		selectedValues.length === 0 ? 'checked' : ''
	}>
			<span>All</span>
			<a href="#" class="clear-link" onclick="clear${
				type.charAt(0).toUpperCase() + type.slice(1)
			}Filters()">Clear</a>
		</label>
	`;

	allItems.forEach((item) => {
		const count = counts[item] || 0;
		const isChecked = selectedValues.includes(item) ? 'checked' : '';
		if (type === 'category') {
			const id = getResearchCategoryIdFromName(item);
			const detailLink = id
				? `<a href="/dashboard/insight.html?id=${id}" target="_blank" class="detail-link" style="margin-left:8px;font-size:12px;color:#2278d0;text-decoration:none;">chi tiết</a>`
				: '';
			html += `<label class="filter-option"><input type="checkbox" name="${type}" value="${item}" ${isChecked}><span>${item}</span><span class="count">${count}</span>${detailLink}</label>`;
		} else {
			html += `<label class="filter-option"><input type="checkbox" name="${type}" value="${item}" ${isChecked}><span>${item}</span><span class="count">${count}</span></label>`;
		}
	});

	// Add "Show less" button
	html += `<div class="show-more" onclick="toggleShowMore('${type}')">Show less</div>`;

	listElement.innerHTML = html;

	// Re-add event listeners
	document.querySelectorAll(`input[name="${type}"]`).forEach((input) => {
		input.addEventListener('change', async () => {
			if (type === 'seller') {
				await onSellerFilterChange.call(input);
			} else if (type === 'category') {
				await onCategoryFilterChange.call(input);
			}
		});
	});
}

function showLimitedItems(type, allItems) {
	const listElement = document.getElementById(type + 'List');
	if (!listElement) return;

	// Preserve selected filters
	const selectedValues = preserveSelectedFilters(type);

	// Get counts
	const counts = type === 'seller' ? getSellerCounts() : getCategoryCounts();

	// Rebuild HTML with limited items
	let html = `
		<label class="filter-option">
			<input type="checkbox" name="${type}" value="all" ${
		selectedValues.length === 0 ? 'checked' : ''
	}>
			<span>All</span>
			<a href="#" class="clear-link" onclick="clear${
				type.charAt(0).toUpperCase() + type.slice(1)
			}Filters()">Clear</a>
		</label>
	`;

	const visibleItems = allItems.slice(0, 5);
	visibleItems.forEach((item) => {
		const count = counts[item] || 0;
		const isChecked = selectedValues.includes(item) ? 'checked' : '';
		if (type === 'category') {
			const id = getResearchCategoryIdFromName(item);
			const detailLink = id
				? `<a href="/dashboard/insight.html?id=${id}" target="_blank" class="detail-link" style="margin-left:8px;font-size:12px;color:#2278d0;text-decoration:none;">chi tiết</a>`
				: '';
			html += `<label class="filter-option"><input type="checkbox" name="${type}" value="${item}" ${isChecked}><span>${item}</span><span class="count">${count}</span>${detailLink}</label>`;
		} else {
			html += `<label class="filter-option"><input type="checkbox" name="${type}" value="${item}" ${isChecked}><span>${item}</span><span class="count">${count}</span></label>`;
		}
	});

	// Add "Show more" button if there are more items
	if (allItems.length > 5) {
		html += `<div class="show-more" onclick="toggleShowMore('${type}')">Show more (${
			allItems.length - 5
		})</div>`;
	}

	listElement.innerHTML = html;

	// Re-add event listeners
	document.querySelectorAll(`input[name="${type}"]`).forEach((input) => {
		input.addEventListener('change', async () => {
			if (type === 'seller') {
				await onSellerFilterChange.call(input);
			} else if (type === 'category') {
				await onCategoryFilterChange.call(input);
			}
		});
	});
}

function clearSellerFilters() {
	document.querySelectorAll('input[name="seller"]').forEach((input) => {
		input.checked = false;
	});
	document.querySelector('input[name="seller"][value="all"]').checked = true;
}

function clearCategoryFilters() {
	document.querySelectorAll('input[name="category"]').forEach((input) => {
		input.checked = false;
	});
	document.querySelector(
		'input[name="category"][value="all"]'
	).checked = true;
}

// Update sidebar counts after products are loaded
function updateSidebarCounts() {
	if (!window.products || window.products.length === 0) {
		console.log('No products available for counting');
		return;
	}

	// console.log(
	// 	'Updating sidebar counts for',
	// 	window.products.length,
	// 	'products'
	// );

	// Update seller counts
	const sellerCounts = getSellerCounts();
	const sellerCountElements = document.querySelectorAll('#sellerList .count');
	sellerCountElements.forEach((element) => {
		const sellerName = element.previousElementSibling.textContent;
		const count = sellerCounts[sellerName] || 0;
		element.textContent = count;
	});

	// Update category counts
	const categoryCounts = getCategoryCounts();
	const categoryCountElements = document.querySelectorAll(
		'#categoryList .count'
	);
	categoryCountElements.forEach((element) => {
		const categoryName = element.previousElementSibling.textContent;
		const count = categoryCounts[categoryName] || 0;
		element.textContent = count;
	});

	// console.log('Updated seller counts:', sellerCounts);
	// console.log('Updated category counts:', categoryCounts);
}

// Helper function to preserve selected filters when rebuilding sidebar
function preserveSelectedFilters(type) {
	const selectedValues = [];
	document
		.querySelectorAll(`input[name="${type}"]:checked`)
		.forEach((input) => {
			if (input.value !== 'all') {
				selectedValues.push(input.value);
			}
		});
	return selectedValues;
}

// Helper function to restore selected filters after rebuilding sidebar
function restoreSelectedFilters(type, selectedValues) {
	document.querySelectorAll(`input[name="${type}"]`).forEach((input) => {
		if (input.value === 'all') {
			input.checked = selectedValues.length === 0;
		} else {
			input.checked = selectedValues.includes(input.value);
		}
	});
}
async function loadSidebar() {
	try {
		const [sellers, categories, sources, researchCats] = await Promise.all([
			fetchSoldByList(),
			fetchCategoryList(),
			fetchSourceList(),
			fetchResearchCategories(),
		]);

		// Store all items globally for show more functionality
		window.allSellers = sellers;
		window.allCategories = categories;
		window.allSources = sources;

		// Store research categories globally for mapping to insight links
		window.researchCategories = Array.isArray(researchCats)
			? researchCats
			: [];

		renderSidebar(sellers, categories, sources);
	} catch (error) {
		console.error('Error loading sidebar:', error);
		showError('Failed to load filter options');
	}
}
async function onSellerFilterChange() {
	// Handle "All" checkbox logic
	const allCheckbox = document.querySelector(
		'input[name="seller"][value="all"]'
	);
	const sellerCheckboxes = Array.from(
		document.querySelectorAll('input[name="seller"]:not([value="all"])')
	);

	if (this.value === 'all' && this.checked) {
		// If "All" is checked, uncheck others
		sellerCheckboxes.forEach((cb) => (cb.checked = false));
	} else if (this.value !== 'all' && this.checked) {
		// If any other is checked, uncheck "All"
		allCheckbox.checked = false;
	}

	// If none are checked, check "All"
	const anyChecked = sellerCheckboxes.some((cb) => cb.checked);
	if (!anyChecked) {
		allCheckbox.checked = true;
	}

	// Get selected sellers
	window.selectedSellers = [];
	if (allCheckbox.checked) {
		window.selectedSellers = []; // Empty array means all sellers
	} else {
		sellerCheckboxes.forEach((cb) => {
			if (cb.checked) window.selectedSellers.push(cb.value);
		});
	}

	await filterProducts();
}

async function onCategoryFilterChange() {
	// Handle "All" checkbox logic
	const allCheckbox = document.querySelector(
		'input[name="category"][value="all"]'
	);
	const categoryCheckboxes = Array.from(
		document.querySelectorAll('input[name="category"]:not([value="all"])')
	);

	if (this.value === 'all' && this.checked) {
		// If "All" is checked, uncheck others
		categoryCheckboxes.forEach((cb) => (cb.checked = false));
	} else if (this.value !== 'all' && this.checked) {
		// If any other is checked, uncheck "All"
		allCheckbox.checked = false;
	}

	// If none are checked, check "All"
	const anyChecked = categoryCheckboxes.some((cb) => cb.checked);
	if (!anyChecked) {
		allCheckbox.checked = true;
	}

	// Get selected categories
	window.selectedCategories = [];
	if (allCheckbox.checked) {
		window.selectedCategories = []; // Empty array means all categories
	} else {
		categoryCheckboxes.forEach((cb) => {
			if (cb.checked) window.selectedCategories.push(cb.value);
		});
	}

	await filterProducts();
}

async function onSourceFilterChange() {
	// Handle "All" checkbox logic
	const allCheckbox = document.querySelector(
		'input[name="source"][value="all"]'
	);
	const sourceCheckboxes = Array.from(
		document.querySelectorAll('input[name="source"]:not([value="all"])')
	);

	if (this.value === 'all' && this.checked) {
		// If "All" is checked, uncheck others
		sourceCheckboxes.forEach((cb) => (cb.checked = false));
	} else if (this.value !== 'all' && this.checked) {
		// If any other is checked, uncheck "All"
		allCheckbox.checked = false;
	}

	// If none are checked, check "All"
	const anyChecked = sourceCheckboxes.some((cb) => cb.checked);
	if (!anyChecked) {
		allCheckbox.checked = true;
	}

	// Get selected sources
	window.selectedSources = [];
	if (allCheckbox.checked) {
		window.selectedSources = []; // Empty array means all sources
	} else {
		sourceCheckboxes.forEach((cb) => {
			if (cb.checked) window.selectedSources.push(cb.value);
		});
	}

	await filterProducts();
}

async function updateStats(products, filteredProducts) {
	// Get total count from API (without any filters)
	let url = `${window.API_BASE_URL || ''}/api/products/count`;
	const res = await fetch(url);
	const data = await res.json();
	const totalProducts = data.count;

	const showingProducts = filteredProducts ? filteredProducts.length : 0;
	const withPrice = filteredProducts
		? filteredProducts.filter((p) => p.price && p.price > 0).length
		: 0;
	const withRating = filteredProducts
		? filteredProducts.filter((p) => p.rating && p.rating > 0).length
		: 0;

	// Update DOM elements
	const totalElement = document.getElementById('total-products-count');
	const showingElement = document.getElementById('showing-products-count');
	const priceElement = document.getElementById('with-price-count');
	const ratingElement = document.getElementById('with-rating-count');

	if (totalElement) totalElement.textContent = totalProducts;
	if (showingElement) showingElement.textContent = showingProducts;
	if (priceElement) priceElement.textContent = withPrice;
	if (ratingElement) ratingElement.textContent = withRating;
}

// Helper function to get currently filtered products (without rendering)
async function getCurrentFilteredProducts() {
	let filtered = window.products || [];

	// Filter by search input
	const searchInput = document.getElementById('searchInput');
	if (searchInput && searchInput.value.trim()) {
		const searchTerm = searchInput.value.toLowerCase().trim();

		// Try different field names
		filtered = filtered.filter((p) => {
			const name = p.name || p.productName || p.title || '';
			const brand = p.brand || '';
			const asin = p.asin || '';

			return (
				name.toLowerCase().includes(searchTerm) ||
				brand.toLowerCase().includes(searchTerm) ||
				asin.toLowerCase().includes(searchTerm)
			);
		});
	}

	// Filter by selected sellers
	if (window.selectedSellers && window.selectedSellers.length > 0) {
		filtered = filtered.filter((p) =>
			window.selectedSellers.includes(p.soldBy)
		);
	}

	// Filter by selected categories
	if (window.selectedCategories && window.selectedCategories.length > 0) {
		filtered = filtered.filter((p) =>
			window.selectedCategories.includes(p.category)
		);
	}

	// Filter by selected sources
	if (window.selectedSources && window.selectedSources.length > 0) {
		filtered = filtered.filter((p) =>
			window.selectedSources.includes(p.source)
		);
	}

	// Apply advanced filters
	filtered = applyAdvancedFilters(filtered);

	return filtered;
}

async function filterProducts() {
	let filtered = await getCurrentFilteredProducts();

	// console.log('Total products available:', window.products?.length || 0);
	// console.log('Filtered products:', filtered.length);
	if (filtered.length > 0) {
		// console.log('Sample filtered product:', filtered[0]);
	}

	// Update stats
	await updateStats(window.products, filtered);

	// Warm up fee rule caches for FBA fee display
	await ensureFeeRuleCaches();

	// Render the filtered products
	if (displayMode === 'grid') {
		await renderGrid(filtered);
	} else {
		await renderTable(filtered);
	}
}
async function loadProducts() {
	try {
		showLoading(true);
		const products = await fetchProducts();
		// console.log('Loaded products from API:', products.length || 0);
		window.products = products || [];

		// Update sidebar counts after products are loaded
		updateSidebarCounts();

		// Use filterProducts to handle stats and rendering
		await filterProducts();
	} catch (error) {
		console.error('Error loading products:', error);
		showError('Failed to load products');
	} finally {
		showLoading(false);
	}
}

// ===== FBA Fee computation helpers =====
let cacheFbaFeeRules = null;
let cacheSizeTierRules = null;

async function ensureFeeRuleCaches() {
	if (!cacheFbaFeeRules) {
		try {
			const r = await fetch(
				`${window.API_BASE_URL || ''}/api/fba-fee-rules`
			);
			cacheFbaFeeRules = r.ok ? await r.json() : [];
		} catch {
			cacheFbaFeeRules = [];
		}
	}
	if (!cacheSizeTierRules) {
		try {
			const r = await fetch(
				`${window.API_BASE_URL || ''}/api/size-tier-rules`
			);
			cacheSizeTierRules = r.ok ? await r.json() : [];
			// console.log(cacheSizeTierRules);
		} catch {
			cacheSizeTierRules = [];
		}
	}
}

function toInches(val, unit) {
	if (val == null) return null;
	const u = String(unit || '').toLowerCase();
	if (u.includes('cm')) return val / 2.54;
	if (u.includes('mm')) return val / 25.4;
	if (u.includes('in')) return val;
	return val; // default inches
}

function toPounds(val, unit) {
	if (val == null) return null;
	const u = String(unit || '').toLowerCase();
	if (u === 'kg' || u.includes('kilogram') || u.includes('kilograms'))
		return val * 2.20462;
	if (u === 'g' || u.includes('gram') || u.includes('grams'))
		return val / 453.592;
	if (u === 'oz' || u.includes('ounce') || u.includes('ounces'))
		return val / 16;
	if (u === 'lb' || u.includes('pound') || u.includes('pounds')) return val;
	return val; // assume lb
}

function computeLengthGirth(dim) {
	if (!dim) return null;
	const L = toInches(dim.length, dim.unit);
	const W = toInches(dim.width, dim.unit);
	const H = toInches(dim.height, dim.unit);
	const arr = [L, W, H].filter((x) => typeof x === 'number');
	if (arr.length < 2) return null;

	// Sort from largest to smallest
	arr.sort((a, b) => b - a);
	const longest = arr[0];

	// Handle case where we only have 2 dimensions (W and H, no L)
	if (arr.length === 2) {
		const shortest = arr[1];
		const girth = 2 * shortest;
		return {
			longest,
			median: null, // No median when only 2 dimensions
			shortest,
			lengthPlusGirth: longest + girth,
		};
	}

	// Handle case where we have 3 dimensions (L, W, H)
	const rest = arr.slice(1);
	const girth = 2 * rest.reduce((s, v) => s + v, 0);
	return {
		longest,
		median: rest[0] ?? null,
		shortest: rest[1] ?? null,
		lengthPlusGirth: longest + girth,
	};
}

function classifyTier(weightLb, dim) {
	if (!Array.isArray(cacheSizeTierRules) || cacheSizeTierRules.length === 0)
		return null;
	const stats = computeLengthGirth(dim) || {};

	let dimensionalWeight = null;
	if (dim.length == null || dim.width == null || dim.height == null) {
		dimensionalWeight = weightLb;
	} else {
		dimensionalWeight = (dim.length * dim.width * dim.height) / 139;
	}

	let shippingWeight = null;
	if (dimensionalWeight != null && dimensionalWeight > weightLb) {
		shippingWeight = dimensionalWeight;
	} else {
		shippingWeight = weightLb;
	}

	console.log(shippingWeight);

	// console.log(stats);
	// Try to find first rule satisfied by all provided constraints

	for (const r of cacheSizeTierRules) {
		// console.log(r);
		const okWeight =
			r.shippingWeightMax == null ||
			(shippingWeight != null && shippingWeight <= r.shippingWeightMax);
		const okLongest =
			r.longestMax == null ||
			(stats.longest != null && stats.longest <= r.longestMax);
		const okShortest =
			r.shortestMax == null ||
			(stats.shortest != null && stats.shortest <= r.shortestMax);
		const okLG =
			r.lengthGirthMax == null ||
			(stats.lengthPlusGirth != null &&
				stats.lengthPlusGirth <= r.lengthGirthMax);

		let okMedian = false;
		if (stats.median != null && r.medianMax != null) {
			okMedian = stats.median <= r.medianMax;
		} else if (stats.median == null) {
			okMedian = true;
		}

		// console.log(okMedian);
		// console.log(okWeight);
		// console.log(okLongest);
		// console.log(okShortest);
		// console.log(okLG);

		if (okMedian && okWeight && okLongest && okShortest && okLG) {
			return normalizeTierName(r.tier);
		}
	}
	// If none matched, return the last tier (largest)
	return normalizeTierName(
		cacheSizeTierRules[cacheSizeTierRules.length - 1].tier
	);
}

function normalizeTierName(t) {
	if (!t) return t;
	const s = String(t).toLowerCase();
	// console.log(s);
	// Map Size Tier Rules to FBA Fee Rules tier names
	if (s.includes('small') && s.includes('standard')) return 'Small Standard';
	if (s.includes('large') && s.includes('standard')) return 'Large Standard';
	return 'Oversize';
}

function lookupFbaFee(tier, weightLb) {
	// console.log('Weight Lb:' + weightLb);
	// console.log('Tier:' + tier);
	if (!Array.isArray(cacheFbaFeeRules) || !tier || weightLb == null)
		return null;
	// consider both lb and oz rules
	const weightOz = weightLb * 16;
	const candidates = cacheFbaFeeRules.filter(
		(r) => r.tier === tier && (r.feeUSD != null || r.baseUSD != null)
	);
	// console.log(candidates);
	let best = null;
	for (const r of candidates) {
		const unit = r.unit || 'oz';
		const w = unit === 'oz' ? weightOz : weightLb;
		const min = r.weightMin || 0;
		const max = r.weightMax == null ? Infinity : r.weightMax;
		console.log('w: ' + w);
		if (w >= min && w <= max) {
			best = r;
			break;
		}
	}
	// console.log(best);
	return best
		? best.feeUSD
			? best.feeUSD
			: best.baseUSD
			? best.baseUSD
			: null
		: 10;
}

function computeFbaFeeDisplay(p) {
	try {
		const weight = p.weight || null;
		const weightVal = weight
			? toPounds(weight.value, weight.unit || '')
			: null;
		// console.log('weight: ' + weightVal);
		const dim = p.dimensions || null;
		const tier = classifyTier(weightVal, dim);
		// console.log(tier);
		const fee = lookupFbaFee(tier, weightVal);
		// console.log(fee);
		if (fee != null) return `$${fee}`;
		return null;
	} catch {
		return null;
	}
}

function computeReferralFeeDisplay(p) {
	try {
		const category = p.category || '';
		const price = p.price || 0;

		// console.log('Referral Fee Debug - Category:', category);
		// console.log('Referral Fee Debug - Price:', price);
		// console.log(
		// 	'Referral Fee Debug - Rules count:',
		// 	cacheReferralFeeRules.length
		// );
		// console.log('Referral Fee Debug - Rules array:', cacheReferralFeeRules);
		// console.log(
		// 	'Referral Fee Debug - Is array?',
		// 	Array.isArray(cacheReferralFeeRules)
		// );

		// Check if rules are loaded
		if (
			!Array.isArray(cacheReferralFeeRules) ||
			cacheReferralFeeRules.length === 0
		) {
			console.log(
				'Referral Fee Debug - Rules not loaded yet, returning null'
			);
			return null;
		}

		if (!category || !price) {
			return null;
		}

		const fee = calculateReferralFee(category, price);
		if (fee != null) {
			return `$${fee.toFixed(2)}`;
		}
		return null;
	} catch (error) {
		console.error('Error calculating referral fee:', error);
		return null;
	}
}
function showGrid() {
	const regularList = document.getElementById('productList');
	const regularTable = document.getElementById('productTable');
	const bestsellersList = document.getElementById('bestsellersProductList');
	const bestsellersTable = document.getElementById('bestsellersProductTable');

	if (currentTab === 'bestsellers') {
		if (bestsellersList) bestsellersList.style.display = 'grid';
		if (bestsellersTable) bestsellersTable.style.display = 'none';
	} else {
		if (regularList) regularList.style.display = 'grid';
		if (regularTable) regularTable.style.display = 'none';
	}
}

function showTable() {
	const regularList = document.getElementById('productList');
	const regularTable = document.getElementById('productTable');
	const bestsellersList = document.getElementById('bestsellersProductList');
	const bestsellersTable = document.getElementById('bestsellersProductTable');

	if (currentTab === 'bestsellers') {
		if (bestsellersList) bestsellersList.style.display = 'none';
		if (bestsellersTable) bestsellersTable.style.display = 'block';
	} else {
		if (regularList) regularList.style.display = 'none';
		if (regularTable) regularTable.style.display = 'block';
	}
}
// Tab switching function
async function switchTab(tabName) {
	currentTab = tabName;

	// Update tab buttons
	document.querySelectorAll('.tab-btn').forEach((btn) => {
		btn.classList.remove('active');
	});
	document.getElementById(tabName + 'Tab').classList.add('active');

	// Update tab content
	document.querySelectorAll('.tab-content').forEach((content) => {
		content.classList.remove('active');
	});
	document.getElementById(tabName + 'TabContent').classList.add('active');

	// Reload data for the new tab
	if (currentTab === 'feerule') {
		await loadFeeRulesUI();
	} else if (currentTab === 'fbafee') {
		await loadFbaFeeRulesUI();
	} else if (currentTab === 'sizetier') {
		await loadSizeTierRulesUI();
	} else {
		await loadSidebar();
		await loadProducts();
	}
}

// Wrapper function for HTML onclick
function switchTabWrapper(tabName) {
	switchTab(tabName).catch((error) => {
		console.error('Error switching tab:', error);
	});
}

async function renderGrid(products) {
	showGrid();
	const list = document.getElementById(
		currentTab === 'bestsellers' ? 'bestsellersProductList' : 'productList'
	);
	if (!list) return;
	// apply image-only layout classes
	list.classList.remove('img-only-4', 'img-only-5', 'img-only-6');
	if (imageOnlyCols === 4) list.classList.add('img-only-4');
	if (imageOnlyCols === 5) list.classList.add('img-only-5');
	if (imageOnlyCols === 6) list.classList.add('img-only-6');
	list.innerHTML = '';
	if (!products.length) {
		list.innerHTML = '<p>No products found.</p>';
		// Update stats even when no products
		await updateStats(window.products, []);
		return;
	}
	products.forEach((p, i) => {
		// Tạo container thay vì link trực tiếp để có thể thêm nút xóa
		const card = document.createElement('div');
		card.className =
			'product-card-v2' + (imageOnlyCols ? ' image-only' : '');
		card.setAttribute('data-id', p._id);
		card.setAttribute('data-product-name', p.productName);

		// Tạo link riêng để mở sản phẩm
		const productLink = document.createElement('a');
		productLink.href = p.url || '#';
		productLink.target = '_blank';
		productLink.style.textDecoration = 'none';
		productLink.style.color = 'inherit';
		productLink.style.display = 'block';
		productLink.style.width = '100%';
		productLink.style.height = '100%';

		// Tạo nút xóa
		const deleteBtn = document.createElement('button');
		deleteBtn.className = 'delete-product-btn';
		deleteBtn.innerHTML = '&times;'; // X symbol
		deleteBtn.title = 'Delete this product';
		deleteBtn.onclick = function (e) {
			e.preventDefault();
			e.stopPropagation();
			showDeleteModal(p._id, p.productName);
		};

		// Determine if it's FBA or FBM based on seller info
		// In real data we might not have this info, so we'll use a placeholder
		const soldBy = p.soldBy || '';
		const fulfillmentType = soldBy.toLowerCase().includes('amazon')
			? 'FBA'
			: 'FBM';
		const fulfillmentClass = fulfillmentType === 'FBA' ? 'fba' : 'fbm';

		// Calculate days since update based on updatedAt field if available
		let daysSinceUpdate = '';
		if (p.updatedAt) {
			const updateDate = new Date(p.date_first_available);
			const currentDate = new Date();
			const diffTime = Math.abs(currentDate - updateDate);
			daysSinceUpdate = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
		}

		// Get rank information from the API data
		const rankKitchen = p.rank_kitchen || '';
		const rankTumbler = p.rank_tumbler || '';

		// Get best rank to display
		let bestRank = '';
		let rankCategory = '';

		if (rankTumbler && rankTumbler !== rankKitchen) {
			bestRank = rankTumbler;
			rankCategory = 'Tumblers & Water Glasses';
		} else if (rankKitchen) {
			bestRank = rankKitchen;
			rankCategory = 'Kitchen & Dining';
		}

		// Get latest best_sellers_rank data if available in new format
		let latestRanks = [];
		if (
			p.best_sellers_rank &&
			Array.isArray(p.best_sellers_rank) &&
			p.best_sellers_rank.length > 0
		) {
			// Sort by date descending to get the latest entry
			const sortedRankEntries = [...p.best_sellers_rank].sort(
				(a, b) => new Date(b.date) - new Date(a.date)
			);

			// Get the latest entry
			const latestEntry = sortedRankEntries[0];
			if (latestEntry && latestEntry.ranks) {
				latestRanks = latestEntry.ranks;
			}
		}

		// Helper function để format số với dấu phẩy
		const formatNumber = (num) => {
			if (!num) return '0';
			return num.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ',');
		};

		// Chuẩn bị nội dung HTML cho product link theo giao diện mới
		productLink.innerHTML = `
            <!-- Header: Brand + Rank Badge + Time -->
            <div class="card-header-v2">
                <div class="header-left">
                    <span class="brand-name-v2">${
						p.brand || p.soldBy || 'Unknown Brand'
					}</span>
                    <div class="rank-badge">#${p.rank_b || i + 1}</div>
                </div>
                ${
					daysSinceUpdate
						? `<span class="time-indicator">${daysSinceUpdate}d</span>`
						: ''
				}
            </div>

            <!-- Hình ảnh sản phẩm -->
            <div class="product-image-v2">
                <img src="${p.productImage || ''}" alt="product image" />
            </div>

            <!-- ASIN + Search + FBA -->
            <div class="asin-row">
                <span class="asin-text">${p.asin || ''}</span>
                ${
					p.soldBy
						? `<span class="fba-badge-v2 ${fulfillmentClass}">${fulfillmentType}</span>`
						: ''
				}
            </div>

            <!-- Best Sellers Rank với category -->
            ${
				latestRanks && latestRanks.length > 0
					? `<div class="ranks-section">
                        ${latestRanks
							.slice(0, 2)
							.map(
								(rankObj) =>
									`<div class="rank-item-v2"><span class="highlight">#${formatNumber(
										rankObj.rank
									)}</span> in ${rankObj.category}</div>`
							)
							.join('')}
                    </div>`
					: ''
			}

            <!-- Bottom section: Unit Sold + Sales trend -->
            <div class="bottom-section">
                <div class="unit-sold sales-trend">${
					p.boughtInLast30Days
						? formatNumber(p.boughtInLast30Days) + '+'
						: '0'
				} in last 30 days</div>
            </div>

            <!-- Rating và Price cùng hàng -->
            <div class="rating-price-row-v2">
                <div class="rating-v2">⭐ ${p.rating || '0.0'} (${formatNumber(
			p.totalReviews || 0
		)})</div>
                ${p.price ? `<div class="price-v2">$${p.price}</div>` : ''}
            </div>
            ${
				computeFbaFeeDisplay(p)
					? `<div class="fba-fee-row">FBA Fee: <span class="fee-price">${computeFbaFeeDisplay(
							p
					  )}</span></div>`
					: ''
			}
            ${
				computeReferralFeeDisplay(p)
					? `<div class="referral-fee-row">Referral Fee: <span class="fee-price">${computeReferralFeeDisplay(
							p
					  )}</span></div>`
					: ''
			}
        `;

		// Thêm các phần tử vào DOM
		card.appendChild(productLink);
		card.appendChild(deleteBtn);
		list.appendChild(card);
	});
}
async function renderTable(products) {
	showTable();
	const tableDiv = document.getElementById(
		currentTab === 'bestsellers'
			? 'bestsellersProductTable'
			: 'productTable'
	);
	if (!tableDiv) return;
	if (!products.length) {
		tableDiv.style.display = 'none';
		// Update stats even when no products
		await updateStats(window.products, []);
		return;
	}
	let html = `<table class="product-table">
        <thead>
            <tr>
                <th>Image</th>
                <th>Name</th>
                <th>Brand</th>
                <th>Rank</th>
                <th>ASIN</th>
                <th>Category</th>
                <th>Price</th>
                <th>Rating</th>
                <th>Reviews</th>
                <th>Sold By</th>
                <th>Best Sellers Rank</th>
                <th>Date First Available</th>
                <th>Link</th>
            </tr>
        </thead>
        <tbody>`;
	products.forEach((p) => {
		// Format date if available
		let formattedDate = '';
		if (p.date_first_available) {
			const date = new Date(p.date_first_available);
			formattedDate = date.toLocaleDateString();
		}

		// Get latest best_sellers_rank data if available in new format
		let latestRanks = [];
		if (
			p.best_sellers_rank &&
			Array.isArray(p.best_sellers_rank) &&
			p.best_sellers_rank.length > 0
		) {
			// Sort by date descending to get the latest entry
			const sortedRankEntries = [...p.best_sellers_rank].sort(
				(a, b) => new Date(b.date) - new Date(a.date)
			);

			// Get the latest entry
			const latestEntry = sortedRankEntries[0];
			if (latestEntry && latestEntry.ranks) {
				latestRanks = latestEntry.ranks;
			}
		}

		// Helper function to truncate text
		const truncateText = (text, maxLength = 30) => {
			if (!text) return '';
			return text.length > maxLength
				? text.slice(0, maxLength) + '...'
				: text;
		};

		html += `<tr>
            <td><img src="${
				p.productImage || ''
			}" alt="Product Image" class="table-product-image" onerror="this.src='data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iNjAiIGhlaWdodD0iNDAiIHZpZXdCb3g9IjAgMCA2MCA0MCIgZmlsbD0ibm9uZSIgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzIwMDAvc3ZnIj4KPHJlY3Qgd2lkdGg9IjYwIiBoZWlnaHQ9IjQwIiBmaWxsPSIjRjhGOUZBIi8+CjxwYXRoIGQ9Ik0yMCAxNUg0MFYyNUgyMFYxNVoiIGZpbGw9IiNEOUQ5REQiLz4KPC9zdmc+'" /></td>
            <td><span class="table-text-truncate" title="${
				p.productName || ''
			}">${truncateText(p.productName || '', 40)}</span></td>
            <td><span class="table-text-truncate" title="${
				p.brand || p.brand_table || ''
			}">${truncateText(p.brand || p.brand_table || '', 20)}</span></td>
            <td class="table-rank">${p.rank_b ? '#' + p.rank_b : '-'}</td>
            <td><span class="table-text-truncate" title="${
				p.asin || ''
			}">${truncateText(p.asin || '', 15)}</span></td>
            <td><span class="table-text-truncate" title="${
				p.category || ''
			}">${truncateText(p.category || '', 25)}</span></td>
            <td class="table-price">${p.price ? '$' + p.price : '-'}</td>
            <td class="table-rating">${p.rating ? '⭐ ' + p.rating : '-'}</td>
            <td>${p.totalReviews || '-'}</td>
            <td><span class="table-text-truncate" title="${
				p.soldBy || ''
			}">${truncateText(p.soldBy || '', 20)}</span></td>
            <td>
                ${
					latestRanks && latestRanks.length > 0
						? latestRanks
								.slice(0, 3) // Show only first 3 ranks
								.map((rankObj) => {
									const rankText = `#${rankObj.rank} in ${rankObj.category}`;
									return `<div class="rank-item" title="${rankText}">${truncateText(
										rankText,
										25
									)}</div>`;
								})
								.join('') +
						  (latestRanks.length > 3
								? `<div class="rank-item">+${
										latestRanks.length - 3
								  } more</div>`
								: '')
						: '-'
				}
            </td>
            <td>${formattedDate || '-'}</td>
            <td><a href="${
				p.url || '#'
			}" target="_blank" class="table-link">View</a></td>
        </tr>`;
	});
	html += `</tbody></table>`;
	tableDiv.innerHTML = html;
	tableDiv.style.display = 'block';
}

// Hàm trích xuất số từ best_sellers_rank
function extractRankNumber(rankString) {
	if (!rankString) return Number.MAX_SAFE_INTEGER; // Nếu không có rank, đặt giá trị cao nhất

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

async function sortProducts(sortBy = null) {
	if (!sortBy) {
		const sortSelect = document.getElementById('sortBy');
		sortBy = sortSelect ? sortSelect.value : '';
	}

	// Get currently filtered products instead of all products
	let sortedProducts = await getCurrentFilteredProducts();
	if (sortBy === 'price_asc') {
		sortedProducts.sort((a, b) => (a.price || 0) - (b.price || 0));
	} else if (sortBy === 'price_desc') {
		sortedProducts.sort((a, b) => (b.price || 0) - (a.price || 0));
	} else if (sortBy === 'date_asc') {
		sortedProducts.sort(
			(a, b) =>
				new Date(a.date_first_available || '1970-01-01') -
				new Date(b.date_first_available || '1970-01-01')
		);
	} else if (sortBy === 'date_desc') {
		sortedProducts.sort(
			(a, b) =>
				new Date(b.date_first_available || '1970-01-01') -
				new Date(a.date_first_available || '1970-01-01')
		);
	} else if (sortBy === 'bought_desc') {
		sortedProducts.sort(
			(a, b) => (b.boughtInLast30Days || 0) - (a.boughtInLast30Days || 0)
		);
	} else if (sortBy === 'bought_asc') {
		sortedProducts.sort(
			(a, b) => (a.boughtInLast30Days || 0) - (b.boughtInLast30Days || 0)
		);
	} else if (sortBy === 'rank_asc') {
		// Sắp xếp theo rank_b tăng dần (rank thấp = tốt hơn)
		sortedProducts.sort((a, b) => {
			const rankA = a.rank_b || Number.MAX_SAFE_INTEGER;
			const rankB = b.rank_b || Number.MAX_SAFE_INTEGER;
			return rankA - rankB;
		});
	} else if (sortBy === 'rank_desc') {
		// Sắp xếp theo rank_b giảm dần
		sortedProducts.sort((a, b) => {
			const rankA = a.rank_b || 0;
			const rankB = b.rank_b || 0;
			return rankB - rankA;
		});
	} else if (sortBy === 'rank_kitchen_asc') {
		// Sắp xếp theo thứ hạng trong danh mục Kitchen & Dining
		sortedProducts.sort(
			(a, b) =>
				getRankInCategory(a.best_sellers_rank, 'Kitchen & Dining') -
				getRankInCategory(b.best_sellers_rank, 'Kitchen & Dining')
		);
	} else if (sortBy === 'rank_tumbler_asc') {
		// Sắp xếp theo thứ hạng trong danh mục Tumblers
		sortedProducts.sort(
			(a, b) =>
				getRankInCategory(
					a.best_sellers_rank,
					'Tumblers & Water Glasses'
				) -
				getRankInCategory(
					b.best_sellers_rank,
					'Tumblers & Water Glasses'
				)
		);
	}

	// Update stats with sorted products (keep original total for stats)
	await updateStats(window.products, sortedProducts);

	if (displayMode === 'grid') {
		await renderGrid(sortedProducts);
	} else {
		await renderTable(sortedProducts);
	}
}

window.selectedSellers = [];
window.selectedCategories = [];
window.selectedSources = [];

// Utility functions for UI feedback
function showError(message) {
	// Create or update error message
	let errorDiv = document.getElementById('error-message');
	if (!errorDiv) {
		errorDiv = document.createElement('div');
		errorDiv.id = 'error-message';
		errorDiv.style.cssText = `
			position: fixed;
			top: 20px;
			right: 20px;
			background: #f44336;
			color: white;
			padding: 12px 20px;
			border-radius: 8px;
			box-shadow: 0 4px 12px rgba(0,0,0,0.15);
			z-index: 1000;
			max-width: 300px;
		`;
		document.body.appendChild(errorDiv);
	}
	errorDiv.textContent = message;
	errorDiv.style.display = 'block';

	// Auto hide after 5 seconds
	setTimeout(() => {
		errorDiv.style.display = 'none';
	}, 5000);
}

function showSuccess(message) {
	// Create or update success message
	let successDiv = document.getElementById('success-message');
	if (!successDiv) {
		successDiv = document.createElement('div');
		successDiv.id = 'success-message';
		successDiv.style.cssText = `
			position: fixed;
			top: 20px;
			right: 20px;
			background: #4caf50;
			color: white;
			padding: 12px 20px;
			border-radius: 8px;
			box-shadow: 0 4px 12px rgba(0,0,0,0.15);
			z-index: 1000;
			max-width: 300px;
		`;
		document.body.appendChild(successDiv);
	}
	successDiv.textContent = message;
	successDiv.style.display = 'block';

	// Auto hide after 3 seconds
	setTimeout(() => {
		successDiv.style.display = 'none';
	}, 3000);
}

function showLoading(show = true) {
	let loadingDiv = document.getElementById('loading-indicator');
	if (show && !loadingDiv) {
		loadingDiv = document.createElement('div');
		loadingDiv.id = 'loading-indicator';
		loadingDiv.style.cssText = `
			position: fixed;
			top: 50%;
			left: 50%;
			transform: translate(-50%, -50%);
			background: rgba(255,255,255,0.9);
			padding: 20px;
			border-radius: 8px;
			box-shadow: 0 4px 12px rgba(0,0,0,0.15);
			z-index: 1000;
			text-align: center;
		`;
		loadingDiv.innerHTML = `
			<div style="
				width: 40px;
				height: 40px;
				border: 4px solid #f3f3f3;
				border-top: 4px solid #ef6c00;
				border-radius: 50%;
				animation: spin 1s linear infinite;
				margin: 0 auto 10px;
			"></div>
			<div>Loading...</div>
		`;
		document.body.appendChild(loadingDiv);
	} else if (!show && loadingDiv) {
		loadingDiv.remove();
	}
}

// Add CSS for loading animation
const style = document.createElement('style');
style.textContent = `
	@keyframes spin {
		0% { transform: rotate(0deg); }
		100% { transform: rotate(360deg); }
	}
`;
document.head.appendChild(style);

document.getElementById('gridBtn').onclick = async () => {
	displayMode = 'grid';
	await renderGrid(window.products || []);
};
document.getElementById('tableBtn').onclick = async () => {
	displayMode = 'table';
	await renderTable(window.products || []);
};

// Xử lý xóa sản phẩm
let currentProductId = null;
const deleteModal = document.getElementById('deleteModal');
const deleteProductName = document.getElementById('deleteProductName');
const closeModal = document.querySelector('.close-modal');
const cancelDelete = document.getElementById('cancelDelete');
const confirmDelete = document.getElementById('confirmDelete');

function showDeleteModal(productId, productName) {
	currentProductId = productId;
	deleteProductName.textContent = productName;
	deleteModal.style.display = 'block';
}

function hideDeleteModal() {
	deleteModal.style.display = 'none';
	currentProductId = null;
}

async function deleteProduct(productId) {
	try {
		const response = await fetch(
			`${window.API_BASE_URL || ''}/api/products/${productId}`,
			{
				method: 'DELETE',
				headers: {
					'Content-Type': 'application/json',
				},
			}
		);

		if (response.ok) {
			// Xóa sản phẩm khỏi mảng products
			window.products = window.products.filter(
				(p) => p._id !== productId
			);
			// Cập nhật lại giao diện và stats
			await filterProducts();

			// Hiển thị thông báo thành công
			alert('Product deleted successfully');
		} else {
			const errorData = await response.json();
			throw new Error(errorData.error || 'Failed to delete product');
		}
	} catch (error) {
		console.error('Error deleting product:', error);
		alert('Failed to delete product: ' + error.message);
	}
}

// Thêm event listeners cho modal
if (closeModal) closeModal.addEventListener('click', hideDeleteModal);
if (cancelDelete) cancelDelete.addEventListener('click', hideDeleteModal);
if (confirmDelete) {
	confirmDelete.addEventListener('click', () => {
		if (currentProductId) {
			deleteProduct(currentProductId);
			hideDeleteModal();
		}
	});
}

// Đóng modal khi click bên ngoài
window.addEventListener('click', (event) => {
	if (event.target === deleteModal) {
		hideDeleteModal();
	}
});

// Dropdown Functions
function toggleDropdown(type) {
	const dropdown = document.getElementById(type + 'Dropdown');
	const otherDropdown =
		type === 'sort'
			? document.getElementById('filterDropdown')
			: document.getElementById(
					type === 'filter' ? 'sortDropdown' : 'filterDropdown'
			  );

	// Close other dropdown
	if (otherDropdown) {
		otherDropdown.classList.remove('show');
	}

	// Toggle current dropdown
	dropdown.classList.toggle('show');
}

// Close dropdowns when clicking outside
document.addEventListener('click', (event) => {
	if (!event.target.closest('.dropdown')) {
		document.querySelectorAll('.dropdown-content').forEach((dropdown) => {
			dropdown.classList.remove('show');
		});
	}
});

// Image-only controls
function selectImageOnly(cols) {
	const text = document.getElementById('imageOnlyText');
	const dd = document.getElementById('imageonlyDropdown');
	imageOnlyCols = cols || 0;
	text.textContent = imageOnlyCols
		? `Image only • ${imageOnlyCols}×`
		: 'Image only';
	if (dd) dd.classList.remove('show');
	// re-render current grid to apply classes
	if (displayMode === 'grid') {
		getCurrentFilteredProducts().then((prods) => renderGrid(prods));
	}
}

// Filter Panel Controls
let currentFilters = {
	bought: 'all',
	newArrival: 'all',
	fulfillment: 'all',
	fbaFee: 'all',
	price: 'all',
	referralFee: 'all',
	rating: 'all',
};

const DEFAULT_FILTERS = {
	bought: 'all',
	newArrival: 'all',
	fulfillment: 'all',
	fbaFee: 'all',
	price: 'all',
	referralFee: 'all',
	rating: 'all',
};

let filterTemplates = [];
let activeTemplate = null;

function toggleFilterPanel() {
	const overlay = document.getElementById('filterOverlay');
	const panel = document.getElementById('filterPanel');

	overlay.classList.add('show');
	panel.classList.add('show');
	document.body.style.overflow = 'hidden';
}

function closeFilterPanel() {
	const overlay = document.getElementById('filterOverlay');
	const panel = document.getElementById('filterPanel');

	overlay.classList.remove('show');
	panel.classList.remove('show');
	document.body.style.overflow = '';
}

function toggleFilterSection(sectionName) {
	const section = document
		.querySelector(`#${sectionName}FilterContent`)
		.closest('.filter-section');
	const isExpanded = section.classList.contains('expanded');

	// Close all sections first
	document
		.querySelectorAll('.filter-section')
		.forEach((s) => s.classList.remove('expanded'));

	// Toggle current section
	if (!isExpanded) {
		section.classList.add('expanded');
	}
}

function updateAppliedFilters() {
	const tagsContainer = document.getElementById('appliedFiltersTags');
	const appliedFilters = [];

	// Check each filter type
	Object.entries(currentFilters).forEach(([key, value]) => {
		if (value !== 'all') {
			let label = '';
			switch (key) {
				case 'bought':
					label = `Bought ≥${value}`;
					break;
				case 'newArrival':
					label = `New Arrival: ${value} days`;
					break;
				case 'fulfillment':
					label = `Fulfillment: ${value}`;
					break;
				case 'fbaFee':
					label = `FBA Fee: ${value}`;
					break;
				case 'price':
					label = `Price: ${value}`;
					break;
				case 'referralFee':
					label = `Referral Fee: ${value}`;
					break;
				case 'rating':
					label = `Rating: ${value}`;
					break;
			}
			appliedFilters.push({ key, value, label });
		}
	});

	// Render applied filter tags
	if (appliedFilters.length === 0) {
		tagsContainer.innerHTML =
			'<p style="color: #999; font-size: 0.9rem; margin: 0;">No filters applied</p>';
	} else {
		tagsContainer.innerHTML = appliedFilters
			.map(
				(filter) => `
            <div class="filter-tag">
                <span>${filter.label}</span>
                <button class="remove-tag" onclick="removeFilter('${filter.key}')">×</button>
            </div>
        `
			)
			.join('');
	}
}

function removeFilter(filterKey) {
	currentFilters[filterKey] = 'all';

	// Reset radio button
	const radio = document.querySelector(
		`input[name="${filterKey}Filter"][value="all"]`
	);
	if (radio) radio.checked = true;

	updateAppliedFilters();
}

function clearAllFilters() {
	currentFilters = {
		bought: 'all',
		newArrival: 'all',
		fulfillment: 'all',
		fbaFee: 'all',
		price: 'all',
		referralFee: 'all',
		rating: 'all',
	};

	// Reset all radio buttons
	document.querySelectorAll('input[type="radio"]').forEach((radio) => {
		if (radio.value === 'all') radio.checked = true;
	});

	updateAppliedFilters();
}

function resetAllFilters() {
	clearAllFilters();
}

function applyAllFilters() {
	// Get current filter values
	Object.keys(currentFilters).forEach((filterKey) => {
		const selected = document.querySelector(
			`input[name="${filterKey}Filter"]:checked`
		);
		if (selected) {
			currentFilters[filterKey] = selected.value;
		}
	});

	updateAppliedFilters();
	closeFilterPanel();

	// Apply filters to products
	applyFiltersToProducts();
}

function applyFiltersToProducts() {
	// This function will filter the products based on currentFilters
	if (displayMode === 'grid') {
		getCurrentFilteredProducts().then((prods) => renderGrid(prods));
	} else {
		getCurrentFilteredProducts().then((prods) => renderTable(prods));
	}
}

function applyAdvancedFilters(products) {
	if (!products) return [];

	return products.filter((product) => {
		// Bought filter
		if (currentFilters.bought !== 'all') {
			const boughtThreshold = parseInt(currentFilters.bought);
			const boughtValue = product.boughtInLast30Days;
			if (!boughtValue || boughtValue < boughtThreshold) {
				return false;
			}
		}

		// New Arrival filter
		if (currentFilters.newArrival !== 'all') {
			const daysThreshold = parseInt(currentFilters.newArrival);
			const dateFirstAvailable = product.date_first_available;
			if (dateFirstAvailable) {
				const productDate = new Date(dateFirstAvailable);
				const now = new Date();
				const daysDiff = (now - productDate) / (1000 * 60 * 60 * 24);
				if (daysDiff > daysThreshold) {
					return false;
				}
			} else {
				return false; // No date available
			}
		}

		// Fulfillment filter
		if (currentFilters.fulfillment !== 'all') {
			const soldBy = product.soldBy;
			if (!soldBy) return false;

			if (currentFilters.fulfillment === 'FBA') {
				if (!soldBy.toLowerCase().includes('amazon')) {
					return false;
				}
			} else if (currentFilters.fulfillment === 'FBM') {
				if (soldBy.toLowerCase().includes('amazon')) {
					return false;
				}
			}
		}

		// FBA Fee filter (if we have FBA fee calculation)
		if (currentFilters.fbaFee !== 'all') {
			const fbaFeeText = computeFbaFeeDisplay(product);
			if (!fbaFeeText) return false;

			const fbaFeeMatch = fbaFeeText.match(/\$([\d.]+)/);
			if (fbaFeeMatch) {
				const fbaFee = parseFloat(fbaFeeMatch[1]);
				const [min, max] = currentFilters.fbaFee
					.split('-')
					.map((v) => (v === '+' ? Infinity : parseFloat(v)));

				if (fbaFee < min || (max !== undefined && fbaFee > max)) {
					return false;
				}
			}
		}

		// Price filter
		if (currentFilters.price !== 'all') {
			const price = product.price;
			if (!price) return false;

			const [min, max] = currentFilters.price
				.split('-')
				.map((v) => (v === '+' ? Infinity : parseFloat(v)));

			if (price < min || (max !== undefined && price > max)) {
				return false;
			}
		}

		// Referral Fee filter
		if (currentFilters.referralFee !== 'all') {
			const referralFeeText = computeReferralFeeDisplay(product);
			if (!referralFeeText) return false;

			const referralFeeMatch = referralFeeText.match(/\$([\d.]+)/);
			if (referralFeeMatch) {
				const referralFee = parseFloat(referralFeeMatch[1]);
				const [min, max] = currentFilters.referralFee
					.split('-')
					.map((v) => (v === '+' ? Infinity : parseFloat(v)));

				if (
					referralFee < min ||
					(max !== undefined && referralFee > max)
				) {
					return false;
				}
			}
		}

		// Rating filter
		if (currentFilters.rating !== 'all') {
			const rating = parseFloat(product.rating);
			if (!rating) return false;

			const minRating = parseFloat(
				currentFilters.rating.replace('+', '')
			);
			if (rating < minRating) {
				return false;
			}
		}

		return true;
	});
}

// Template Management Functions
async function fetchFilterTemplates() {
	try {
		const res = await fetch(
			`${window.API_BASE_URL || ''}/api/filter-templates`
		);
		if (!res.ok) throw new Error(`HTTP ${res.status}`);
		filterTemplates = await res.json();
		return filterTemplates;
	} catch (e) {
		console.error('Error fetching filter templates:', e);
		showError('Failed to load filter templates');
		return [];
	}
}

function renderTemplateList(templates) {
	const templateList = document.getElementById('templateList');
	if (!templateList) return;

	if (!templates || templates.length === 0) {
		templateList.innerHTML =
			'<p style="color: #999; font-size: 0.9rem; margin: 0;">No templates available</p>';
		return;
	}

	templateList.innerHTML = templates
		.map(
			(template) => `
		<div class="template-item ${activeTemplate === template._id ? 'active' : ''}"
			 onclick="applyTemplate('${template._id}')">
			<div>
				<div class="template-name">${template.name}</div>
				${
					template.description
						? `<div class="template-description">${template.description}</div>`
						: ''
				}
			</div>
			<div class="template-actions">
				<button class="template-delete-btn" onclick="deleteTemplate('${
					template._id
				}')" title="Delete template">
					×
				</button>
			</div>
		</div>
	`
		)
		.join('');
}

async function applyTemplate(templateId) {
	try {
		const template = filterTemplates.find((t) => t._id === templateId);
		if (!template) return;
		const state = template.state || {};

		// Restore filters (fallback to legacy template.filters)
		const raw = { ...(state.filters || template.filters || {}) };
		const merged = { ...DEFAULT_FILTERS, ...raw };
		Object.keys(DEFAULT_FILTERS).forEach((k) => {
			if (
				merged[k] === undefined ||
				merged[k] === null ||
				merged[k] === ''
			) {
				merged[k] = 'all';
			}
		});
		currentFilters = merged;
		Object.keys(currentFilters).forEach((filterKey) => {
			const value = currentFilters[filterKey];
			const radio = document.querySelector(
				`input[name="${filterKey}Filter"][value="${value}"]`
			);
			if (radio) radio.checked = true;
		});
		updateAppliedFilters();

		// Restore seller/category/source selections
		const setChecks = (name, values) => {
			const inputs = document.querySelectorAll(`input[name="${name}"]`);
			if (!inputs.length) return [];
			const selected = new Set(Array.isArray(values) ? values : []);
			let selectedValues = [];
			inputs.forEach((i) => {
				if (i.value === 'all') {
					i.checked = selected.size === 0;
				} else {
					i.checked = selected.has(i.value);
					if (i.checked) selectedValues.push(i.value);
				}
			});
			return selectedValues;
		};
		window.selectedSellers = setChecks('seller', state.sellers);
		window.selectedCategories = setChecks('category', state.categories);
		window.selectedSources = setChecks('source', state.sources);

		// Restore sort
		if (state.sort) {
			const sortOptions = {
				'': 'Sort by',
				price_asc: 'Price: Low to High',
				price_desc: 'Price: High to Low',
				date_asc: 'Date: Oldest to Newest',
				date_desc: 'Date: Newest to Oldest',
				bought_desc: 'Bought: High to Low',
				bought_asc: 'Bought: Low to High',
				rank_asc: 'Rank: Low to High',
				rank_desc: 'Rank: High to Low',
			};
			const sortText = document.getElementById('sortText');
			if (sortText)
				sortText.textContent = sortOptions[state.sort] || 'Sort by';
			await sortProducts(state.sort);
		}

		// Restore display mode and image-only
		if (typeof state.imageOnlyCols === 'number') {
			imageOnlyCols = state.imageOnlyCols;
		}
		if (state.displayMode) {
			switchView(state.displayMode);
		}

		// Set active and refresh UI
		activeTemplate = templateId;
		renderTemplateList(filterTemplates);
		const dropdown = document.getElementById('quickFilterDropdown');
		if (dropdown) dropdown.classList.remove('show');
		const quickFilterText = document.getElementById('quickFilterText');
		if (quickFilterText) quickFilterText.textContent = 'Save Template';
	} catch (e) {
		console.error('Error applying template:', e);
		showError('Failed to apply template');
	}
}

async function saveCurrentFiltersAsTemplate() {
	const templateNameInput = document.getElementById('templateName');
	if (!templateNameInput) return;

	const templateName = templateNameInput.value.trim();
	if (!templateName) {
		showError('Please enter a template name');
		return;
	}

	// Check if template name already exists
	const existingTemplate = filterTemplates.find(
		(t) => t.name.toLowerCase() === templateName.toLowerCase()
	);
	if (existingTemplate) {
		showError('Template name already exists');
		return;
	}

	try {
		// Collect sidebar selections
		const getCheckedValues = (name) =>
			Array.from(
				document.querySelectorAll(
					`input[name="${name}"]:not([value="all"])`
				)
			)
				.filter((i) => i.checked)
				.map((i) => i.value);

		// Map sort label to value
		const sortTextLabel =
			document.getElementById('sortText')?.textContent?.trim() || '';
		const reverseSortMap = {
			'Price: Low to High': 'price_asc',
			'Price: High to Low': 'price_desc',
			'Date: Oldest to Newest': 'date_asc',
			'Date: Newest to Oldest': 'date_desc',
			'Bought: High to Low': 'bought_desc',
			'Bought: Low to High': 'bought_asc',
			'Rank: Low to High': 'rank_asc',
			'Rank: High to Low': 'rank_desc',
			'Sort by': '',
		};
		const sortValue = reverseSortMap[sortTextLabel] ?? '';

		const res = await fetch(
			`${window.API_BASE_URL || ''}/api/filter-templates`,
			{
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({
					name: templateName,
					filters: currentFilters,
					state: {
						filters: currentFilters,
						sellers: getCheckedValues('seller'),
						categories: getCheckedValues('category'),
						sources: getCheckedValues('source'),
						sort: sortValue,
						displayMode,
						imageOnlyCols,
					},
					description: `Custom template: ${templateName}`,
				}),
			}
		);

		if (!res.ok) throw new Error(`HTTP ${res.status}`);

		// Clear input
		templateNameInput.value = '';

		// Refresh templates
		await fetchFilterTemplates();
		renderTemplateList(filterTemplates);

		showSuccess('Template saved successfully');
	} catch (e) {
		console.error('Error saving template:', e);
		showError('Failed to save template');
	}
}

async function deleteTemplate(templateId) {
	if (!confirm('Delete this template?')) return;

	try {
		const res = await fetch(
			`${window.API_BASE_URL || ''}/api/filter-templates/${templateId}`,
			{
				method: 'DELETE',
			}
		);

		if (!res.ok) throw new Error(`HTTP ${res.status}`);

		// Remove from local array
		filterTemplates = filterTemplates.filter((t) => t._id !== templateId);

		// Clear active template if it was deleted
		if (activeTemplate === templateId) {
			activeTemplate = null;
			const quickFilterText = document.getElementById('quickFilterText');
			if (quickFilterText) {
				quickFilterText.textContent = 'Quick Filter';
			}
		}

		// Re-render template list
		renderTemplateList(filterTemplates);

		showSuccess('Template deleted successfully');
	} catch (e) {
		console.error('Error deleting template:', e);
		showError('Failed to delete template');
	}
}

async function loadFilterTemplates() {
	await fetchFilterTemplates();
	renderTemplateList(filterTemplates);
}

// Sort Functions
function selectSort(value) {
	const sortText = document.getElementById('sortText');
	const sortDropdown = document.getElementById('sortDropdown');

	// Update text based on selection
	const sortOptions = {
		'': 'Sort by',
		price_asc: 'Price: Low to High',
		price_desc: 'Price: High to Low',
		date_asc: 'Date: Oldest to Newest',
		date_desc: 'Date: Newest to Oldest',
		bought_desc: 'Bought: High to Low',
		bought_asc: 'Bought: Low to High',
		rank_asc: 'Rank: Low to High',
		rank_desc: 'Rank: High to Low',
	};

	sortText.textContent = sortOptions[value] || 'Sort by';
	sortDropdown.classList.remove('show');

	// Apply sort
	if (value) {
		sortProducts(value);
	}
}

// Filter Functions
function applyFilters() {
	const filterDropdown = document.getElementById('filterDropdown');
	filterDropdown.classList.remove('show');

	// Get filter values
	const minPrice = document.getElementById('minPrice').value;
	const maxPrice = document.getElementById('maxPrice').value;
	const startDate = document.getElementById('startDate').value;
	const endDate = document.getElementById('endDate').value;
	const boughtInPastMonth =
		document.getElementById('boughtInPastMonth').checked;

	// Apply filters (you can implement the actual filtering logic here)
	loadProducts();
}

function resetFilters() {
	// Reset all filter inputs
	document.getElementById('minPrice').value = '';
	document.getElementById('maxPrice').value = '';
	document.getElementById('startDate').value = '';
	document.getElementById('endDate').value = '';
	document.getElementById('boughtInPastMonth').checked = false;

	// Close dropdown
	document.getElementById('filterDropdown').classList.remove('show');

	// Reload products
	loadProducts();
}

// View Switch Functions
function switchView(view) {
	const gridBtn = document.getElementById('gridBtn');
	const tableBtn = document.getElementById('tableBtn');
	const productList = document.getElementById('productList');
	const productTable = document.getElementById('productTable');

	if (view === 'grid') {
		gridBtn.classList.add('active');
		tableBtn.classList.remove('active');
		productList.style.display = 'grid';
		productTable.style.display = 'none';
	} else {
		tableBtn.classList.add('active');
		gridBtn.classList.remove('active');
		productList.style.display = 'none';
		productTable.style.display = 'block';
	}
}

// Add event listeners
document.addEventListener('DOMContentLoaded', () => {
	const searchInput = document.getElementById('searchInput');
	if (searchInput) {
		// Add debounced search
		let searchTimeout;
		searchInput.addEventListener('input', () => {
			clearTimeout(searchTimeout);
			searchTimeout = setTimeout(() => {
				filterProducts();
			}, 300); // 300ms delay
		});
	}
});

// Test function to check database
async function testDatabase() {
	try {
		const res = await fetch(
			`${window.API_BASE_URL || ''}/api/products/test`
		);
		const data = await res.json();
		// console.log('Database test result:', data);
		return data;
	} catch (error) {
		console.error('Database test failed:', error);
		return null;
	}
}

window.onload = async () => {
	// Test database first
	await testDatabase();

	// Load referral fee rules FIRST for calculation
	await fetchReferralFeeRules();

	await loadSidebar();
	await loadProducts();

	// Apply default sort: Date: Newest to Oldest
	const sortText = document.getElementById('sortText');
	if (sortText) sortText.textContent = 'Date: Newest to Oldest';
	await sortProducts('date_desc');
	await loadFilterTemplates();
};

// ===== Fee Rule Tab Logic =====
async function fetchFeeRules() {
	try {
		const url = `${window.API_BASE_URL || ''}/api/fee-rules`;
		const res = await fetch(url);
		if (!res.ok) throw new Error(`HTTP ${res.status}`);
		return await res.json();
	} catch (err) {
		console.error('Error fetching fee rules:', err);
		showError('Failed to load fee rules');
		return [];
	}
}

function renderFeeRuleTable(rules) {
	const tableDiv = document.getElementById('feeRuleTable');
	if (!tableDiv) return;
	if (!rules || rules.length === 0) {
		tableDiv.innerHTML = '<p>No fee rules found.</p>';
		return;
	}
	let html = `<table class="product-table">
        <thead>
            <tr>
                <th>Category</th>
                <th>Price Min</th>
                <th>Price Max</th>
                <th>Apply To</th>
                <th>Fee %</th>
                <th>Min Fee (USD)</th>
                <th>Variant</th>
                <th>Actions</th>
            </tr>
        </thead>
        <tbody>`;
	rules.forEach((r) => {
		html += `<tr>
            <td>${r.category || ''}</td>
            <td>${r.priceMin ?? ''}</td>
            <td>${r.priceMax ?? ''}</td>
            <td>${r.applyTo || ''}</td>
            <td>${r.feePercent ?? ''}</td>
            <td>${r.minFeeUSD ?? ''}</td>
            <td>${r.variant || ''}</td>
            <td>
                <button class="reset-btn" onclick="editFeeRule('${
					r._id
				}')">Edit</button>
                <button class="modal-btn modal-btn-delete" onclick="deleteFeeRule('${
					r._id
				}')">Delete</button>
            </td>
        </tr>`;
	});
	html += `</tbody></table>`;
	tableDiv.innerHTML = html;
}

function openFeeRuleForm(rule = null) {
	const existing = rule || {};
	const form = document.getElementById('feeRuleInlineForm');
	if (!form) return;
	ensureFeeRuleFields();
	// Populate fields
	document.getElementById('fr_id').value = existing._id || '';
	document.getElementById('fr_category').value = existing.category || '';
	document.getElementById('fr_priceMin').value = existing.priceMin ?? '';
	document.getElementById('fr_priceMax').value = existing.priceMax ?? 0;
	document.getElementById('fr_applyTo').value = (
		existing.applyTo || 'total'
	).toLowerCase();
	document.getElementById('fr_feePercent').value = existing.feePercent ?? '';
	document.getElementById('fr_minFeeUSD').value = existing.minFeeUSD ?? 0;
	document.getElementById('fr_variant').value = existing.variant || '';
	form.style.display = 'block';
}

function hideFeeRuleInlineForm() {
	const form = document.getElementById('feeRuleInlineForm');
	if (form) form.style.display = 'none';
}

function editFeeRule(id) {
	// Find rule from current table data
	// For simplicity, refetch single rule
	fetch(`${window.API_BASE_URL || ''}/api/fee-rules/${id}`)
		.then((res) => res.json())
		.then((rule) => openFeeRuleForm(rule))
		.catch((err) => showError('Failed to load rule'));
}

async function submitFeeRuleForm() {
	try {
		const id = document.getElementById('fr_id').value.trim();
		const priceMinRaw = document.getElementById('fr_priceMin').value;
		const priceMaxRaw = document.getElementById('fr_priceMax').value;
		const payload = {
			category: document.getElementById('fr_category').value.trim(),
			priceMin: priceMinRaw === '' ? null : parseFloat(priceMinRaw),
			priceMax:
				!priceMaxRaw || parseFloat(priceMaxRaw) === 0
					? null
					: parseFloat(priceMaxRaw),
			applyTo: (
				document.getElementById('fr_applyTo').value || 'total'
			).toLowerCase(),
			feePercent: parseFloat(
				document.getElementById('fr_feePercent').value
			),
			minFeeUSD: parseFloat(
				document.getElementById('fr_minFeeUSD').value || 0
			),
			variant: document.getElementById('fr_variant').value.trim(),
		};

		const method = id ? 'PUT' : 'POST';
		const url = `${window.API_BASE_URL || ''}/api/fee-rules${
			id ? '/' + id : ''
		}`;
		const res = await fetch(url, {
			method,
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify(payload),
		});
		if (!res.ok) throw new Error(`HTTP ${res.status}`);
		hideFeeRuleInlineForm();
		await reloadFeeRules();
	} catch (err) {
		console.error('Failed to save rule:', err);
		showError('Failed to save rule');
	}
}

async function deleteFeeRule(id) {
	if (!confirm('Delete this rule?')) return;
	try {
		const res = await fetch(
			`${window.API_BASE_URL || ''}/api/fee-rules/${id}`,
			{ method: 'DELETE' }
		);
		if (!res.ok) throw new Error(`HTTP ${res.status}`);
		await reloadFeeRules();
	} catch (err) {
		console.error('Failed to delete rule:', err);
		showError('Failed to delete rule');
	}
}

async function reloadFeeRules() {
	const rules = await fetchFeeRules();
	renderFeeRuleTable(rules);
}

async function loadFeeRulesUI() {
	await reloadFeeRules();
}

// Expose fee rule functions to global scope for inline onclick handlers
window.openFeeRuleForm = openFeeRuleForm;
window.submitFeeRuleForm = submitFeeRuleForm;
window.editFeeRule = editFeeRule;
window.deleteFeeRule = deleteFeeRule;
window.reloadFeeRules = reloadFeeRules;
window.importFeeRulesFromExcel = importFeeRulesFromExcel;

// ===== FBA Fee Rules Tab Logic =====
async function fetchFbaFeeRules() {
	try {
		const url = `${window.API_BASE_URL || ''}/api/fba-fee-rules`;
		const res = await fetch(url);
		if (!res.ok) throw new Error(`HTTP ${res.status}`);
		return await res.json();
	} catch (err) {
		console.error('Error fetching FBA fee rules:', err);
		showError('Failed to load FBA fee rules');
		return [];
	}
}

function renderFbaFeeRuleTable(rules) {
	const tableDiv = document.getElementById('fbaFeeRuleTable');
	if (!tableDiv) return;
	if (!rules || rules.length === 0) {
		tableDiv.innerHTML = '<p>No FBA fee rules found.</p>';
		return;
	}
	let html = `<table class="product-table">
        <thead>
            <tr>
                <th>Tier</th>
                <th>Weight Min</th>
                <th>Weight Max</th>
                <th>Unit</th>
                <th>Fee (USD)</th>
                <th>Base (USD)</th>
                <th>Overage</th>
                <th>Variant</th>
                <th>Actions</th>
            </tr>
        </thead>
        <tbody>`;
	rules.forEach((r) => {
		const over = Array.isArray(r.overageRules) ? r.overageRules : [];
		const overStr = over
			.map((o) => {
				const thr = [o.overThresholdValue, o.overThresholdUnit]
					.filter((v) => v !== undefined && v !== null && v !== '')
					.join(' ');
				const step = [o.stepValue, o.stepUnit]
					.filter((v) => v !== undefined && v !== null && v !== '')
					.join(' ');
				const fee =
					o.stepFeeUSD !== undefined && o.stepFeeUSD !== null
						? `$${o.stepFeeUSD}`
						: '';
				return thr && step && fee
					? `over ${thr}: +${fee} per ${step}`
					: '';
			})
			.filter(Boolean)
			.join('; ');
		html += `<tr>
            <td>${r.tier || ''}</td>
            <td>${r.weightMin ?? ''}</td>
            <td>${r.weightMax ?? ''}</td>
            <td>${r.unit || ''}</td>
            <td>${r.feeUSD ?? ''}</td>
            <td>${r.baseUSD ?? ''}</td>
            <td>${overStr}</td>
            <td>${r.variant || ''}</td>
            <td>
                <button class="reset-btn" onclick="editFbaFeeRule('${
					r._id
				}')">Edit</button>
                <button class="modal-btn modal-btn-delete" onclick="deleteFbaFeeRule('${
					r._id
				}')">Delete</button>
            </td>
        </tr>`;
	});
	html += `</tbody></table>`;
	tableDiv.innerHTML = html;
}

function openFbaFeeForm(rule = null) {
	const existing = rule || {};
	const form = document.getElementById('fbaFeeInlineForm');
	if (!form) return;
	document.getElementById('ff_id').value = existing._id || '';
	document.getElementById('ff_tier').value = existing.tier || '';
	document.getElementById('ff_weightMin').value = existing.weightMin ?? 0;
	document.getElementById('ff_weightMax').value = existing.weightMax ?? 0;
	document.getElementById('ff_unit').value = existing.unit || 'oz';
	document.getElementById('ff_feeUSD').value = existing.feeUSD ?? '';
	document.getElementById('ff_baseUSD').value = existing.baseUSD ?? '';
	const firstOver = (existing.overageRules && existing.overageRules[0]) || {};
	document.getElementById('ff_overThresholdValue').value =
		firstOver.overThresholdValue ?? '';
	document.getElementById('ff_overThresholdUnit').value =
		firstOver.overThresholdUnit || 'lb';
	document.getElementById('ff_stepValue').value = firstOver.stepValue ?? '';
	document.getElementById('ff_stepUnit').value = firstOver.stepUnit || 'lb';
	document.getElementById('ff_stepFeeUSD').value = firstOver.stepFeeUSD ?? '';
	document.getElementById('ff_variant').value = existing.variant || '';
	form.style.display = 'block';
}

function hideFbaFeeInlineForm() {
	const form = document.getElementById('fbaFeeInlineForm');
	if (form) form.style.display = 'none';
}

async function submitFbaFeeForm() {
	try {
		const id = document.getElementById('ff_id').value.trim();
		const payload = {
			tier: document.getElementById('ff_tier').value.trim(),
			weightMin: parseFloat(
				document.getElementById('ff_weightMin').value || 0
			),
			weightMax:
				(parseFloat(
					document.getElementById('ff_weightMax').value || 0
				) || 0) === 0
					? null
					: parseFloat(document.getElementById('ff_weightMax').value),
			unit: document.getElementById('ff_unit').value,
			feeUSD: parseFloat(document.getElementById('ff_feeUSD').value),
			baseUSD: parseFloat(document.getElementById('ff_baseUSD').value),
			variant: document.getElementById('ff_variant').value.trim(),
		};

		// If any overage inputs are present, include overageRules array
		const ovVal = parseFloat(
			document.getElementById('ff_overThresholdValue').value
		);
		const ovUnit = document.getElementById('ff_overThresholdUnit').value;
		const stVal = parseFloat(document.getElementById('ff_stepValue').value);
		const stUnit = document.getElementById('ff_stepUnit').value;
		const stFee = parseFloat(
			document.getElementById('ff_stepFeeUSD').value
		);
		if (!isNaN(ovVal) || !isNaN(stVal) || !isNaN(stFee)) {
			payload.overageRules = [];
			if (
				!isNaN(ovVal) &&
				!isNaN(stVal) &&
				!isNaN(stFee) &&
				ovUnit &&
				stUnit
			) {
				payload.overageRules.push({
					overThresholdValue: ovVal,
					overThresholdUnit: ovUnit,
					stepValue: stVal,
					stepUnit: stUnit,
					stepFeeUSD: stFee,
				});
			}
		}

		const method = id ? 'PUT' : 'POST';
		const url = `${window.API_BASE_URL || ''}/api/fba-fee-rules${
			id ? '/' + id : ''
		}`;
		const res = await fetch(url, {
			method,
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify(payload),
		});
		if (!res.ok) throw new Error(`HTTP ${res.status}`);
		hideFbaFeeInlineForm();
		await reloadFbaFeeRules();
	} catch (err) {
		console.error('Failed to save FBA rule:', err);
		showError('Failed to save FBA rule');
	}
}

function editFbaFeeRule(id) {
	fetch(`${window.API_BASE_URL || ''}/api/fba-fee-rules/${id}`)
		.then((res) => res.json())
		.then((rule) => openFbaFeeForm(rule))
		.catch(() => showError('Failed to load FBA rule'));
}

async function deleteFbaFeeRule(id) {
	if (!confirm('Delete this FBA rule?')) return;
	try {
		const res = await fetch(
			`${window.API_BASE_URL || ''}/api/fba-fee-rules/${id}`,
			{ method: 'DELETE' }
		);
		if (!res.ok) throw new Error(`HTTP ${res.status}`);
		await reloadFbaFeeRules();
	} catch (err) {
		console.error('Failed to delete FBA rule:', err);
		showError('Failed to delete FBA rule');
	}
}

async function reloadFbaFeeRules() {
	const rules = await fetchFbaFeeRules();
	renderFbaFeeRuleTable(rules);
}

async function importFbaFeeRulesFromExcel(input) {
	try {
		const file = input.files && input.files[0];
		if (!file) return;
		showLoading(true);
		const form = new FormData();
		form.append('file', file);
		const res = await fetch(
			`${window.API_BASE_URL || ''}/api/fba-fee-rules/import`,
			{ method: 'POST', body: form }
		);
		if (!res.ok) throw new Error(`HTTP ${res.status}`);
		await reloadFbaFeeRules();
		input.value = '';
	} catch (err) {
		console.error('FBA Import failed:', err);
		showError('FBA Import failed. Please check file format.');
	} finally {
		showLoading(false);
	}
}

async function loadFbaFeeRulesUI() {
	await reloadFbaFeeRules();
}

async function loadSizeTierRulesUI() {
	await reloadSizeTierRules();
}

// Expose to global
window.openFbaFeeForm = openFbaFeeForm;
window.submitFbaFeeForm = submitFbaFeeForm;
window.editFbaFeeRule = editFbaFeeRule;
window.deleteFbaFeeRule = deleteFbaFeeRule;
window.reloadFbaFeeRules = reloadFbaFeeRules;
window.importFbaFeeRulesFromExcel = importFbaFeeRulesFromExcel;

// Ensure ApplyTo is a select with the required options and hint for Price Max
function ensureFeeRuleFields() {
	const applyEl = document.getElementById('fr_applyTo');
	if (applyEl && applyEl.tagName.toLowerCase() !== 'select') {
		const select = document.createElement('select');
		select.id = 'fr_applyTo';
		select.innerHTML =
			'<option value="total">Total</option><option value="portion">Portion</option>';
		applyEl.replaceWith(select);
	}
	const priceMaxEl = document.getElementById('fr_priceMax');
	if (priceMaxEl && !priceMaxEl.placeholder.includes('0 = All')) {
		priceMaxEl.placeholder = 'Max (0 = All)';
	}
}

// ===== Size Tier Rules Tab Logic =====
async function fetchSizeTierRules() {
	try {
		const res = await fetch(
			`${window.API_BASE_URL || ''}/api/size-tier-rules`
		);
		if (!res.ok) throw new Error(`HTTP ${res.status}`);
		return await res.json();
	} catch (e) {
		console.error('Error fetching size tier rules:', e);
		showError('Failed to load size tier rules');
		return [];
	}
}

function renderSizeTierRuleTable(rules) {
	const el = document.getElementById('sizeTierRuleTable');
	if (!el) return;
	if (!rules || rules.length === 0) {
		el.innerHTML = '<p>No size tier rules found.</p>';
		return;
	}
	let html = `<table class="product-table"><thead><tr>
        <th>Tier</th><th>Ship Wt Max</th><th>Longest</th><th>Median</th><th>Shortest</th><th>Len+Girth</th><th>Units</th><th>Variant</th><th>Actions</th>
    </tr></thead><tbody>`;
	rules.forEach((r) => {
		html += `<tr>
            <td>${r.tier || ''}</td>
            <td>${r.shippingWeightMax ?? ''}</td>
            <td>${r.longestMax ?? ''}</td>
            <td>${r.medianMax ?? ''}</td>
            <td>${r.shortestMax ?? ''}</td>
            <td>${r.lengthGirthMax ?? ''}</td>
            <td>${(r.unitWeight || 'lb') + '/' + (r.unitLength || 'in')}</td>
            <td>${r.variant || ''}</td>
            <td>
                <button class="reset-btn" onclick="editSizeTierRule('${
					r._id
				}')">Edit</button>
                <button class="modal-btn modal-btn-delete" onclick="deleteSizeTierRule('${
					r._id
				}')">Delete</button>
            </td>
        </tr>`;
	});
	html += `</tbody></table>`;
	el.innerHTML = html;
}

function openSizeTierForm(rule = null) {
	const r = rule || {};
	const form = document.getElementById('sizeTierInlineForm');
	if (!form) return;
	document.getElementById('st_id').value = r._id || '';
	document.getElementById('st_tier').value = r.tier || '';
	document.getElementById('st_shippingWeightMax').value =
		r.shippingWeightMax ?? '';
	document.getElementById('st_longestMax').value = r.longestMax ?? '';
	document.getElementById('st_medianMax').value = r.medianMax ?? '';
	document.getElementById('st_shortestMax').value = r.shortestMax ?? '';
	document.getElementById('st_lengthGirthMax').value = r.lengthGirthMax ?? '';
	document.getElementById('st_unitLength').value = r.unitLength || 'in';
	document.getElementById('st_unitWeight').value = r.unitWeight || 'lb';
	document.getElementById('st_variant').value = r.variant || '';
	form.style.display = 'block';
}

function hideSizeTierInlineForm() {
	const form = document.getElementById('sizeTierInlineForm');
	if (form) form.style.display = 'none';
}

async function submitSizeTierForm() {
	try {
		const id = document.getElementById('st_id').value.trim();
		const payload = {
			tier: document.getElementById('st_tier').value.trim(),
			shippingWeightMax:
				parseFloat(
					document.getElementById('st_shippingWeightMax').value
				) || null,
			longestMax:
				parseFloat(document.getElementById('st_longestMax').value) ||
				null,
			medianMax:
				parseFloat(document.getElementById('st_medianMax').value) ||
				null,
			shortestMax:
				parseFloat(document.getElementById('st_shortestMax').value) ||
				null,
			lengthGirthMax:
				parseFloat(
					document.getElementById('st_lengthGirthMax').value
				) || null,
			unitLength: document.getElementById('st_unitLength').value,
			unitWeight: document.getElementById('st_unitWeight').value,
			variant: document.getElementById('st_variant').value.trim(),
		};
		const method = id ? 'PUT' : 'POST';
		const url = `${window.API_BASE_URL || ''}/api/size-tier-rules${
			id ? '/' + id : ''
		}`;
		const res = await fetch(url, {
			method,
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify(payload),
		});
		if (!res.ok) throw new Error(`HTTP ${res.status}`);
		hideSizeTierInlineForm();
		await reloadSizeTierRules();
	} catch (e) {
		console.error('Failed to save size tier rule:', e);
		showError('Failed to save size tier rule');
	}
}

function editSizeTierRule(id) {
	fetch(`${window.API_BASE_URL || ''}/api/size-tier-rules/${id}`)
		.then((r) => r.json())
		.then((rule) => openSizeTierForm(rule))
		.catch(() => showError('Failed to load size tier rule'));
}

async function deleteSizeTierRule(id) {
	if (!confirm('Delete this size tier rule?')) return;
	try {
		const res = await fetch(
			`${window.API_BASE_URL || ''}/api/size-tier-rules/${id}`,
			{ method: 'DELETE' }
		);
		if (!res.ok) throw new Error(`HTTP ${res.status}`);
		await reloadSizeTierRules();
	} catch (e) {
		console.error('Failed to delete size tier rule:', e);
		showError('Failed to delete size tier rule');
	}
}

async function reloadSizeTierRules() {
	const rules = await fetchSizeTierRules();
	renderSizeTierRuleTable(rules);
}

async function importSizeTierRulesFromExcel(input) {
	try {
		const file = input.files && input.files[0];
		if (!file) return;
		showLoading(true);
		const form = new FormData();
		form.append('file', file);
		const res = await fetch(
			`${window.API_BASE_URL || ''}/api/size-tier-rules/import`,
			{ method: 'POST', body: form }
		);
		if (!res.ok) throw new Error(`HTTP ${res.status}`);
		await reloadSizeTierRules();
		input.value = '';
	} catch (e) {
		console.error('Size tier import failed:', e);
		showError('Size tier import failed. Please check file format.');
	} finally {
		showLoading(false);
	}
}

// expose
window.openSizeTierForm = openSizeTierForm;
window.submitSizeTierForm = submitSizeTierForm;
window.deleteSizeTierRule = deleteSizeTierRule;
window.editSizeTierRule = editSizeTierRule;
window.reloadSizeTierRules = reloadSizeTierRules;
window.importSizeTierRulesFromExcel = importSizeTierRulesFromExcel;

// Filter Panel Functions
window.toggleFilterPanel = toggleFilterPanel;
window.closeFilterPanel = closeFilterPanel;
window.toggleFilterSection = toggleFilterSection;
window.removeFilter = removeFilter;
window.clearAllFilters = clearAllFilters;
window.resetAllFilters = resetAllFilters;
window.applyAllFilters = applyAllFilters;

// Template Functions
window.saveCurrentFiltersAsTemplate = saveCurrentFiltersAsTemplate;
window.applyTemplate = applyTemplate;
window.deleteTemplate = deleteTemplate;
window.loadFilterTemplates = loadFilterTemplates;
window.clearActiveTemplate = async function () {
	// 1) Clear active template selection and UI highlight
	activeTemplate = null;
	renderTemplateList(filterTemplates);

	// 2) Reset filters to defaults
	currentFilters = { ...DEFAULT_FILTERS };
	Object.keys(currentFilters).forEach((filterKey) => {
		const value = currentFilters[filterKey];
		const radio = document.querySelector(
			`input[name="${filterKey}Filter"][value="${value}"]`
		);
		if (radio) radio.checked = true;
	});
	updateAppliedFilters();

	// 3) Reset sidebar selections to All
	const resetGroup = (name) => {
		const inputs = document.querySelectorAll(`input[name="${name}"]`);
		inputs.forEach((i) => (i.checked = i.value === 'all'));
	};
	resetGroup('seller');
	resetGroup('category');
	resetGroup('source');
	window.selectedSellers = [];
	window.selectedCategories = [];
	window.selectedSources = [];

	// 4) Reset sort to default: Date: Newest to Oldest
	const sortText = document.getElementById('sortText');
	if (sortText) sortText.textContent = 'Date: Newest to Oldest';
	await sortProducts('date_desc');

	// 5) Close dropdown and reset button label
	const quickFilterText = document.getElementById('quickFilterText');
	if (quickFilterText) quickFilterText.textContent = 'Save Template';
	const dropdown = document.getElementById('quickFilterDropdown');
	if (dropdown) dropdown.classList.remove('show');

	// 6) Re-filter products with defaults
	await filterProducts();
};

async function importFeeRulesFromExcel(input) {
	try {
		const file = input.files && input.files[0];
		if (!file) return;
		showLoading(true);
		const form = new FormData();
		form.append('file', file);
		const res = await fetch(
			`${window.API_BASE_URL || ''}/api/fee-rules/import`,
			{
				method: 'POST',
				body: form,
			}
		);
		if (!res.ok) throw new Error(`HTTP ${res.status}`);
		await reloadFeeRules();
		input.value = '';
	} catch (err) {
		console.error('Import failed:', err);
		showError('Import failed. Please check file format.');
	} finally {
		showLoading(false);
	}
}

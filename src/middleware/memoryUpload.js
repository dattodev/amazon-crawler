const multer = require('multer');
const XLSX = require('xlsx');

// Configure multer for memory storage (suitable for serverless)
const storage = multer.memoryStorage();
const upload = multer({
	storage: storage,
	limits: {
		fileSize: 10 * 1024 * 1024, // 10MB limit
	},
	fileFilter: (req, file, cb) => {
		// Only allow Excel files
		if (
			file.mimetype ===
				'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' ||
			file.mimetype === 'application/vnd.ms-excel' ||
			file.originalname.endsWith('.xlsx') ||
			file.originalname.endsWith('.xls')
		) {
			cb(null, true);
		} else {
			cb(new Error('Only Excel files (.xlsx, .xls) are allowed'), false);
		}
	},
});

// Middleware to process Excel file from memory buffer
const processExcelFromBuffer = (req, res, next) => {
	if (!req.file) {
		return res.status(400).json({ error: 'No file uploaded' });
	}

	try {
		// Read Excel file from buffer
		const workbook = XLSX.read(req.file.buffer, { type: 'buffer' });

		// Attach workbook to request for use in controllers
		req.excelWorkbook = workbook;
		next();
	} catch (error) {
		console.error('Error processing Excel file:', error);
		return res.status(400).json({
			error: 'Invalid Excel file format',
			details: error.message,
		});
	}
};

module.exports = {
	upload,
	processExcelFromBuffer,
};

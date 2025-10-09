# Amazon Product Dashboard

A comprehensive dashboard for managing and viewing Amazon product data with advanced filtering, sorting, and analytics capabilities.

## Features

- 📊 **Product Management**: View, filter, and manage Amazon products
- 🔍 **Advanced Filtering**: Filter by seller, category, source, price range, and date
- 📈 **Analytics**: Track product statistics and rankings
- 🎨 **Responsive Design**: Works on desktop, tablet, and mobile
- ⚡ **Real-time Updates**: Live data updates and error handling
- 🗂️ **Multiple Views**: Grid and table view options

## Tech Stack

- **Backend**: Node.js, Express.js, MongoDB, Mongoose
- **Frontend**: Vanilla JavaScript, HTML5, CSS3
- **Database**: MongoDB with optimized schemas

## Setup

### Prerequisites
- Node.js (v14 or higher)
- MongoDB (local or cloud instance)
- npm or yarn

### Installation

1. **Install dependencies:**
   ```bash
   npm install
   ```

2. **Environment Configuration:**
   Create a `.env` file in the dashboard directory:
   ```env
   MONGO_URI=mongodb://localhost:27017/dashboard
   PORT=3000
   NODE_ENV=development
   API_BASE_URL=
   CRAWLER_API_ENDPOINT=
   FRONTEND_URL=http://localhost:3000
   ```

3. **Start the server:**
   ```bash
   # Development mode with auto-reload
   npm run dev

   # Production mode
   npm start
   ```

4. **Access the dashboard:**
   - API: http://localhost:3000
   - Dashboard: http://localhost:3000/dashboard

## API Endpoints

### Products
- `GET /api/products` - Get all products with filtering
- `GET /api/products/count` - Get product count
- `GET /api/products/:id` - Get product by ID
- `POST /api/products` - Create new product
- `PUT /api/products/:id` - Update product
- `DELETE /api/products/:id` - Delete product

### Utilities
- `GET /api/products/soldby-list` - Get unique sellers
- `GET /api/products/category-list` - Get unique categories
- `GET /api/products/source-list` - Get unique sources
- `POST /api/products/delete-all` - Delete all products (dangerous!)

## Project Structure

```
dashboard/
├── src/
│   ├── controllers/     # API route handlers
│   ├── models/         # MongoDB schemas
│   ├── routes/         # API routes
│   ├── services/       # Business logic
│   └── view/          # Frontend files
│       ├── dashboard.html
│       ├── dashboard.css
│       └── dashboard.js
├── index.js           # Main server file
├── package.json       # Dependencies
└── README.md         # This file
```

## Usage

### Filtering Products
- Use the sidebar filters to filter by seller, category, or source
- Use the filter bar for price range, date range, and search
- Apply multiple filters simultaneously

### Sorting
- Sort by price (low to high / high to low)
- Sort by date (oldest to newest / newest to oldest)
- Sort by bought in last 30 days

### Views
- **Grid View**: Card-based layout with images
- **Table View**: Compact tabular format

## Development

### Adding New Features
1. Add new routes in `src/routes/`
2. Implement controllers in `src/controllers/`
3. Add business logic in `src/services/`
4. Update frontend in `src/view/`

### Database Schema
The Product model includes:
- Basic info (name, brand, ASIN, price, rating)
- Amazon-specific data (best sellers rank, reviews)
- Metadata (source, timestamps, categories)

## Security Notes

- The `delete-all` endpoint requires POST method for safety
- Input validation should be added for production use
- Consider adding authentication for production deployment

## Troubleshooting

### Common Issues
1. **MongoDB Connection Error**: Check if MongoDB is running and connection string is correct
2. **CORS Issues**: Verify FRONTEND_URL in .env file
3. **Port Already in Use**: Change PORT in .env file

### Logs
Check console output for detailed error messages and connection status.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## License

MIT License - see LICENSE file for details.

# Hướng dẫn Deploy Dashboard lên Vercel

## Tổng quan
Dashboard này đã được cấu hình để chạy trên Vercel serverless environment. Các thay đổi chính bao gồm:

1. **File Upload**: Chuyển từ file system sang memory storage
2. **Static Files**: Cấu hình đường dẫn phù hợp với Vercel
3. **Environment Variables**: Cấu hình cho production

## Các thay đổi đã thực hiện

### 1. Cấu hình Vercel
- ✅ Tạo `vercel.json` với cấu hình routing
- ✅ Cập nhật `package.json` với build scripts
- ✅ Tạo `env.example` với các biến môi trường

### 2. Xử lý File Upload
- ✅ Tạo `src/middleware/memoryUpload.js` để xử lý upload trong memory
- ✅ Cập nhật tất cả routes sử dụng middleware mới
- ✅ Cập nhật controllers để nhận workbook thay vì file path
- ✅ Cập nhật services để xử lý workbook từ memory

### 3. Các file đã được cập nhật
- `src/routes/referralFeeRule.routes.js`
- `src/routes/fbaFeeRule.routes.js`
- `src/routes/sizeTierRule.routes.js`
- `src/controllers/referralFeeRule.controller.js`
- `src/controllers/fbaFeeRule.controller.js`
- `src/controllers/sizeTierRule.controller.js`
- `src/services/referralFeeRule.service.js`
- `src/services/fbaFeeRule.service.js`
- `src/services/sizeTierRule.service.js`

## Hướng dẫn Deploy

### Bước 1: Chuẩn bị Database
1. Tạo MongoDB Atlas cluster (hoặc sử dụng MongoDB cloud khác)
2. Lấy connection string từ MongoDB Atlas
3. Cấu hình IP whitelist để cho phép Vercel access

### Bước 2: Deploy lên Vercel

#### Cách 1: Sử dụng Vercel CLI
```bash
# Cài đặt Vercel CLI
npm i -g vercel

# Login vào Vercel
vercel login

# Deploy project
vercel

# Cấu hình environment variables
vercel env add MONGO_URI
vercel env add NODE_ENV
vercel env add API_BASE_URL
vercel env add CRAWLER_API_ENDPOINT
vercel env add FRONTEND_URL
```

#### Cách 2: Sử dụng GitHub Integration
1. Push code lên GitHub repository
2. Kết nối repository với Vercel
3. Cấu hình environment variables trong Vercel dashboard

### Bước 3: Cấu hình Environment Variables

Trong Vercel dashboard, thêm các biến môi trường sau:

```
MONGO_URI=mongodb+srv://username:password@cluster.mongodb.net/dashboard?retryWrites=true&w=majority
NODE_ENV=production
API_BASE_URL=https://your-domain.vercel.app
CRAWLER_API_ENDPOINT=https://your-crawler-api.com
FRONTEND_URL=https://your-domain.vercel.app
```

### Bước 4: Kiểm tra Deployment

1. Truy cập `https://your-domain.vercel.app` để kiểm tra API
2. Truy cập `https://your-domain.vercel.app/dashboard` để kiểm tra frontend
3. Test chức năng upload Excel files

## Lưu ý quan trọng

### 1. File Upload Limitations
- Vercel có giới hạn 10MB cho request body
- File upload được xử lý trong memory, không lưu trữ persistent
- Chỉ hỗ trợ Excel files (.xlsx, .xls)

### 2. Database Connection
- Sử dụng MongoDB Atlas hoặc cloud database
- Cấu hình connection pooling phù hợp
- Monitor database performance

### 3. Performance
- Vercel có timeout 30 giây cho serverless functions
- Cân nhắc sử dụng database indexing
- Monitor memory usage

### 4. Security
- Cấu hình CORS phù hợp
- Sử dụng HTTPS
- Validate file uploads
- Rate limiting nếu cần

## Troubleshooting

### Lỗi thường gặp:

1. **MongoDB Connection Error**
   - Kiểm tra connection string
   - Kiểm tra IP whitelist
   - Kiểm tra database credentials

2. **File Upload Error**
   - Kiểm tra file size (max 10MB)
   - Kiểm tra file format (.xlsx, .xls only)
   - Kiểm tra memory usage

3. **Static Files Not Loading**
   - Kiểm tra đường dẫn trong `vercel.json`
   - Kiểm tra file permissions
   - Kiểm tra build process

### Debug Commands:
```bash
# Kiểm tra logs
vercel logs

# Kiểm tra environment
vercel env ls

# Redeploy
vercel --prod
```

## Monitoring

1. Sử dụng Vercel Analytics để monitor performance
2. Monitor MongoDB Atlas metrics
3. Set up alerts cho errors
4. Monitor memory usage và response times

## Backup Strategy

1. Regular MongoDB backups
2. Export data định kỳ
3. Version control cho code changes
4. Environment variables backup

---

**Lưu ý**: Dashboard này đã được tối ưu cho Vercel serverless environment. Tất cả file uploads được xử lý trong memory và không lưu trữ persistent trên file system.

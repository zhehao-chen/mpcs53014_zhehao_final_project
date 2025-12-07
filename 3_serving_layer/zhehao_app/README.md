# 芝加哥用电预测系统

## 快速开始

```bash
npm install
node app.js
```

访问 http://localhost:3020

## API 端点

- `/api/predictions?startRow=2025-12-01 00:00:00&endRow=2025-12-01 23:59:59`
- `/api/actual-load?startRow=2025-12-01 00:00:00&endRow=2025-12-01 23:59:59`
- `/api/weather?startRow=2025-12-01 00:00:00&endRow=2025-12-01 23:59:59`
- `/api/combined?date=2025-12-01`

## 环境变量

```bash
HBASE_HOST=ec2-34-230-47-10.compute-1.amazonaws.com
HBASE_PORT=8070
PORT=3020
```

const express = require('express');
const axios = require('axios');

const app = express();
const PORT = process.env.PORT || 3020;
const HBASE_HOST = process.env.HBASE_HOST || 'ec2-34-230-47-10.compute-1.amazonaws.com';
const HBASE_PORT = process.env.HBASE_PORT || '8070';
const HBASE_BASE_URL = `http://${HBASE_HOST}:${HBASE_PORT}`;

app.use(express.json());
app.use(express.static('public'));
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  next();
});

// 解析HBase响应
function parseHBase(data) {
  if (!data.Row) return [];
  return data.Row.map(row => {
    const rowKey = Buffer.from(row.key, 'base64').toString();
    const cells = {};
    row.Cell.forEach(cell => {
      const col = Buffer.from(cell.column, 'base64').toString().split(':')[1];
      cells[col] = Buffer.from(cell.$, 'base64').toString();
    });
    return { rowKey, ...cells };
  });
}

// 获取预测数据
app.get('/api/predictions', async (req, res) => {
  try {
    const { startRow, endRow } = req.query;
    const url = `${HBASE_BASE_URL}/zhehao_daily_predictions_hbase/${startRow},${endRow}`;
    const response = await axios.get(url, { headers: { 'Accept': 'application/json' } });
    const data = parseHBase(response.data).map(item => ({
      datetime: item.rowKey,
      timestamp: parseInt(item.timestamp),
      predictedLoad: parseFloat(item.value)
    }));
    res.json({ success: true, data });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// 获取实际负载
app.get('/api/actual-load', async (req, res) => {
  try {
    const { startRow, endRow } = req.query;
    const url = `${HBASE_BASE_URL}/zhehao_hbase_real_load/${startRow},${endRow}`;
    const response = await axios.get(url, { headers: { 'Accept': 'application/json' } });
    const data = parseHBase(response.data).map(item => ({
      datetime: item.rowKey,
      timestamp: parseInt(item.timestamp_ms),
      actualLoad: parseFloat(item.current_reading)
    }));
    res.json({ success: true, data });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// 获取天气数据
app.get('/api/weather', async (req, res) => {
  try {
    const { startRow, endRow } = req.query;
    const url = `${HBASE_BASE_URL}/zhehao_hbase_real_weather/${startRow},${endRow}`;
    const response = await axios.get(url, { headers: { 'Accept': 'application/json' } });
    const data = parseHBase(response.data).map(item => ({
      datetime: item.rowKey,
      temperature: parseFloat(item.temp),
      humidity: parseFloat(item.hmdt),
      pressure: parseFloat(item.atm_press),
      windSpeed: parseFloat(item.wnd_spd),
      precipitation: parseFloat(item.prcp)
    }));
    res.json({ success: true, data });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// 获取综合数据
app.get('/api/combined', async (req, res) => {
  try {
    const { date } = req.query;
    const startRow = `${date} 00:00:00`;
    const endRow = `${date} 23:59:59`;
    
    const [pred, actual, weather] = await Promise.all([
      axios.get(`${HBASE_BASE_URL}/zhehao_daily_predictions_hbase/${startRow},${endRow}`, 
        { headers: { 'Accept': 'application/json' } }),
      axios.get(`${HBASE_BASE_URL}/zhehao_hbase_real_load/${startRow},${endRow}`, 
        { headers: { 'Accept': 'application/json' } }),
      axios.get(`${HBASE_BASE_URL}/zhehao_hbase_real_weather/${startRow},${endRow}`, 
        { headers: { 'Accept': 'application/json' } })
    ]);
    
    const predictions = parseHBase(pred.data);
    const actualData = parseHBase(actual.data);
    const weatherData = parseHBase(weather.data);
    
    const combined = predictions.map(p => {
      const a = actualData.find(x => x.rowKey === p.rowKey);
      const w = weatherData.find(x => x.rowKey === p.rowKey);
      return {
        datetime: p.rowKey,
        predicted: parseFloat(p.value),
        actual: a ? parseFloat(a.current_reading) : null,
        error: a ? Math.abs(parseFloat(p.value) - parseFloat(a.current_reading)) : null,
        temp: w ? parseFloat(w.temp) : null,
        humidity: w ? parseFloat(w.hmdt) : null,
        windSpeed: w ? parseFloat(w.wnd_spd) : null
      };
    });
    
    res.json({ success: true, data: combined });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

app.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
  console.log(`HBase: ${HBASE_BASE_URL}`);
});

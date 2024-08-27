const labels = [];
const vwapPriceData = [];
const sizePerSecData = [];
const sma5Data = [];

const ctx = document.getElementById("stockChart").getContext("2d");
const stockChart = new Chart(ctx, {
  type: "bar",
  data: {
    labels: labels,
    datasets: [
      {
        label: "Size per Second",
        data: sizePerSecData,
        backgroundColor: "rgba(54, 162, 235, 0.5)",
        yAxisID: "y",
      },
      {
        label: "VWAP Price per Second",
        data: vwapPriceData,
        type: "line",
        borderColor: "rgba(255, 99, 132, 1)",
        borderWidth: 2,
        fill: false,
        yAxisID: "y1",
      },
      {
        label: "5 Period SMA",
        data: sma5Data,
        type: "line",
        borderColor: "rgba(75, 192, 192, 1)",
        borderWidth: 2,
        fill: false,
        yAxisID: "y1",
      },
    ],
  },
  options: {
    scales: {
      y: {
        type: "linear",
        position: "left",
        title: {
          display: true,
          text: "Size",
        },
      },
      y1: {
        type: "linear",
        position: "right",
        title: {
          display: true,
          text: "Price",
        },
      },
      x: {
        title: {
          display: true,
          text: "Time",
        },
      },
    },
  },
});

// WebSocket連接
const ws = new WebSocket("wss://raphaelfang.com:8001/stock/v1/ws/data");

ws.onmessage = function (event) {
  const incomingData = JSON.parse(event.data);

  // 假設資料格式與前述格式一致
  const timeLabel = new Date(incomingData.current_time).toLocaleTimeString();
  labels.push(timeLabel);

  vwapPriceData.push(incomingData.vwap_price_per_sec);
  sizePerSecData.push(incomingData.size_per_sec);

  if (incomingData.sma_5 !== undefined) {
    sma5Data.push(incomingData.sma_5);
  }

  // 更新圖表
  stockChart.update();
};

// 可選：限制圖表上顯示的資料點數量
function limitDataPoints() {
  const maxDataPoints = 50; // 最多顯示的資料點數
  if (labels.length > maxDataPoints) {
    labels.shift();
    vwapPriceData.shift();
    sizePerSecData.shift();
    sma5Data.shift();
  }
}

// 定期限制資料點
setInterval(limitDataPoints, 1000);

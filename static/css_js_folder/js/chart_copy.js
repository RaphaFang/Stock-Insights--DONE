document.addEventListener("DOMContentLoaded", function () {
  // 初始化股票數據物件
  const stocks = {};

  // WebSocket模擬資料
  const incomingData = [
    {
      symbol: "6115",
      type: "per_sec_data",
      start: "2024-09-04T19:55:49.000+08:00",
      end: "2024-09-04T19:55:50.000+08:00",
      current_time: "2024-09-04T19:55:50.978+08:00",
      last_data_time: "2024-09-04T19:55:49.238+08:00",
      real_data_count: 0,
      filled_data_count: 1,
      real_or_filled: "filled",
      vwap_price_per_sec: 200.0,
      size_per_sec: 100,
      yesterday_price: 200.0,
      price_change_percentage: 0.0,
    },
    {
      symbol: "2330",
      type: "MA_data",
      MA_type: "5_MA_data",
      start: "2024-09-04T19:55:42.000+08:00",
      end: "2024-09-04T19:55:47.000+08:00",
      current_time: "2024-09-04T19:55:49.325+08:00",
      first_in_window: "2024-09-04T19:55:42.000+08:00",
      last_in_window: "2024-09-04T19:55:42.000+08:00",
      real_data_count: 0,
      filled_data_count: 1,
      sma_5: 205.0,
      sum_of_vwap: 0.0,
      count_of_vwap: 0,
      "5_data_count": 1,
    },
  ];

  // 處理收到的數據
  incomingData.forEach((data) => {
    const symbol = data.symbol;
    if (!stocks[symbol]) {
      stocks[symbol] = {
        perSecData: [],
        maData: [],
        priceChart: null,
        volumeChart: null,
      };

      // 創建圖表容器
      createCharts(symbol);
    }

    // 根據資料類型進行分類
    if (data.type === "per_sec_data") {
      stocks[symbol].perSecData.push(data);
    } else if (data.type === "MA_data") {
      stocks[symbol].maData.push(data);
    }

    // 更新圖表
    updateCharts(symbol);
  });

  // 創建圖表的容器及圖表
  function createCharts(symbol) {
    const chartsContainer = document.getElementById("charts-container");
    const stockContainer = document.createElement("div");
    stockContainer.id = `stock-${symbol}`;
    stockContainer.innerHTML = `<h2>股票代碼: ${symbol}</h2>
          <canvas id="price-chart-${symbol}"></canvas>
          <canvas id="volume-chart-${symbol}"></canvas>`;
    chartsContainer.appendChild(stockContainer);

    // 初始化圖表
    stocks[symbol].priceChart = new Chart(document.getElementById(`price-chart-${symbol}`).getContext("2d"), {
      type: "line",
      data: {
        labels: [],
        datasets: [
          {
            label: "即時價格",
            data: [],
            borderColor: "blue",
            fill: false,
          },
          {
            label: "移動平均 (SMA_5)",
            data: [],
            borderColor: "green",
            fill: false,
          },
        ],
      },
      options: {
        scales: {
          x: { title: { display: true, text: "時間" } },
          y: { title: { display: true, text: "價格" } },
        },
      },
    });

    stocks[symbol].volumeChart = new Chart(document.getElementById(`volume-chart-${symbol}`).getContext("2d"), {
      type: "bar",
      data: {
        labels: [],
        datasets: [
          {
            label: "每秒交易量",
            data: [],
            backgroundColor: "orange",
          },
        ],
      },
      options: {
        scales: {
          x: { title: { display: true, text: "時間" } },
          y: { title: { display: true, text: "交易量" } },
        },
      },
    });
  }

  // 更新圖表數據
  function updateCharts(symbol) {
    const priceChart = stocks[symbol].priceChart;
    const volumeChart = stocks[symbol].volumeChart;

    const perSecData = stocks[symbol].perSecData;
    const maData = stocks[symbol].maData;

    // 更新折線圖
    priceChart.data.labels = perSecData.map((data) => new Date(data.start).toLocaleTimeString());
    priceChart.data.datasets[0].data = perSecData.map((data) => data.vwap_price_per_sec);
    priceChart.data.datasets[1].data = maData.map((data) => data.sma_5);
    priceChart.update();

    // 更新柱狀圖
    volumeChart.data.labels = perSecData.map((data) => new Date(data.start).toLocaleTimeString());
    volumeChart.data.datasets[0].data = perSecData.map((data) => data.size_per_sec);
    volumeChart.update();
  }
});

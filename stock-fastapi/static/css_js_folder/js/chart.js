document.addEventListener("DOMContentLoaded", function () {
  const stocks = {};

  const socket = new WebSocket("wss://raphaelfang.com/stock/v1/ws/data");

  socket.onmessage = function (event) {
    const data = JSON.parse(event.data);
    const symbol = data.symbol;

    if (!stocks[symbol]) {
      stocks[symbol] = {
        perSecData: [],
        maData: [],
        priceChart: null,
        volumeChart: null,
      };

      createCharts(symbol);
    }

    if (data.type === "per_sec_data") {
      stocks[symbol].perSecData.push(data);
    } else if (data.type === "MA_data") {
      stocks[symbol].maData.push(data);
    }

    updateCharts(symbol);
  };

  function createCharts(symbol) {
    const chartsContainer = document.getElementById("charts-container");
    const stockContainer = document.createElement("div");
    stockContainer.id = `stock-${symbol}`;
    stockContainer.innerHTML = `<h2>股票代碼: ${symbol}</h2>
            <canvas id="price-chart-${symbol}"></canvas>
            <canvas id="volume-chart-${symbol}"></canvas>`;
    chartsContainer.appendChild(stockContainer);
    setTimeout(() => {
      const priceCanvas = document.getElementById(`price-chart-${symbol}`);
      const volumeCanvas = document.getElementById(`volume-chart-${symbol}`);

      if (priceCanvas && volumeCanvas) {
        stocks[symbol].priceChart = new Chart(priceCanvas.getContext("2d"), {
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

        stocks[symbol].volumeChart = new Chart(volumeCanvas.getContext("2d"), {
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
    }, 0);
  }

  function updateCharts(symbol) {
    const priceChart = stocks[symbol].priceChart;
    const volumeChart = stocks[symbol].volumeChart;

    if (priceChart && volumeChart) {
      const perSecData = stocks[symbol].perSecData;
      const maData = stocks[symbol].maData;

      priceChart.data.labels = perSecData.map((data) => new Date(data.start).toLocaleTimeString());
      priceChart.data.datasets[0].data = perSecData.map((data) => data.vwap_price_per_sec);
      priceChart.data.datasets[1].data = maData.map((data) => data.sma_5);
      priceChart.update();

      volumeChart.data.labels = perSecData.map((data) => new Date(data.start).toLocaleTimeString());
      volumeChart.data.datasets[0].data = perSecData.map((data) => data.size_per_sec);
      volumeChart.update();
    }
  }
});

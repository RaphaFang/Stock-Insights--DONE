document.addEventListener("DOMContentLoaded", function () {
  const labels = [];
  const vwapPriceData = [];
  const sma5Data = [];
  const sizePerSecData = [];
  const pricePercentageChange = [];
  const priceColors = [];

  const ctx = document.getElementById("stockChart").getContext("2d");

  // 初始化图表
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
          type: "bar",
        },
        {
          label: "VWAP Price per Second",
          data: vwapPriceData,
          borderColor: function (context) {
            const index = context.dataIndex;
            return priceColors[index] || "rgba(0, 0, 0, 1)";
          },
          borderWidth: 2,
          fill: false,
          yAxisID: "y1",
          type: "line",
        },
        {
          label: "5 Period SMA",
          data: sma5Data,
          borderColor: "rgba(75, 192, 192, 1)",
          borderWidth: 2,
          fill: false,
          yAxisID: "y1",
          type: "line",
        },
      ],
    },
    options: {
      scales: {
        y: {
          type: "linear",
          position: "left",
          min: 0,
          max: 200, // 根据接收到的Size数据动态调整
          title: {
            display: true,
            text: "Size",
          },
        },
        y1: {
          type: "linear",
          position: "right",
          min: 90,
          max: 110, // 根据接收到的VWAP数据动态调整
          title: {
            display: true,
            text: "Price",
          },
        },
        x: {
          type: "time",
          time: {
            unit: "second",
            displayFormats: {
              second: "HH:mm:ss",
            },
          },
          title: {
            display: true,
            text: "Time",
          },
        },
      },
      plugins: {
        zoom: {
          pan: {
            enabled: true,
            mode: "x",
          },
          zoom: {
            wheel: {
              enabled: true,
              mode: "x",
            },
            pinch: {
              enabled: true,
              mode: "x",
            },
          },
        },
      },
    },
  });

  // WebSocket 连接
  const ws = new WebSocket("wss://raphaelfang.com/stock/v1/ws/data");

  ws.onopen = function () {
    console.log("WebSocket connection opened");
  };

  ws.onmessage = function (event) {
    const incomingData = JSON.parse(event.data);
    console.log("Received Data:", incomingData);

    const timeLabel = new Date(incomingData.start).toLocaleTimeString("en-US", { hour12: false });

    // 限制数据点数量
    if (labels.length > 50) {
      labels.shift();
      vwapPriceData.shift();
      sizePerSecData.shift();
      sma5Data.shift();
      pricePercentageChange.shift();
      priceColors.shift();
    }

    labels.push(timeLabel);
    vwapPriceData.push(incomingData.vwap_price_per_sec);
    sizePerSecData.push(incomingData.size_per_sec);

    // 设置颜色
    if (incomingData.price_change_percentage > 0) {
      priceColors.push("rgba(255, 0, 0, 1)"); // 红色表示价格上涨
    } else {
      priceColors.push("rgba(0, 128, 0, 1)"); // 绿色表示价格下跌
    }

    if (incomingData.type === "MA_data" && incomingData.MA_type === "5_MA_data") {
      sma5Data.push(incomingData.sma_5);
    }

    // 更新图表
    stockChart.update();
  };

  ws.onerror = function (error) {
    console.error("WebSocket error: ", error);
  };

  ws.onclose = function () {
    console.log("WebSocket connection closed");
  };
});

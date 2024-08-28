document.addEventListener("DOMContentLoaded", function () {
  const maxDataPoints = 16200; // 最大数据量
  const labels = [];
  const vwapPriceData = [];
  const sma5Data = [];
  const sizePerSecData = [];
  const pricePercentageChange = []; // 用于存储价格变化的百分比
  const priceColors = []; // 用于存储价格变化的颜色

  // 价格图表设置
  const priceCtx = document.getElementById("priceChart").getContext("2d");
  const priceChart = new Chart(priceCtx, {
    type: "line", // 改为折线图
    data: {
      labels: labels,
      datasets: [
        {
          label: "VWAP Price per Second",
          data: vwapPriceData,
          borderColor: "rgba(255, 99, 132, 1)",
          borderWidth: 2,
          fill: false,
          yAxisID: "y1", // 价格
        },
        {
          label: "5 Period SMA",
          data: sma5Data,
          borderColor: "rgba(75, 192, 192, 1)",
          borderWidth: 2,
          fill: false,
          yAxisID: "y1", // 价格
        },
        {
          label: "Price Change (%)",
          data: pricePercentageChange,
          borderColor: function (context) {
            const index = context.dataIndex;
            return priceColors[index]; // 根据变化的颜色显示
          },
          borderWidth: 2,
          fill: false,
          yAxisID: "y2", // 百分比变化
        },
      ],
    },
    options: {
      scales: {
        y1: {
          type: "linear",
          position: "left",
          title: {
            display: true,
            text: "Price",
          },
        },
        y2: {
          type: "linear",
          position: "right",
          title: {
            display: true,
            text: "Percentage Change (%)",
          },
          ticks: {
            // 以0%为中心的正负10%显示
            min: -10,
            max: 10,
            callback: function (value) {
              return value + "%";
            },
          },
        },
        x: {
          type: "time",
          time: {
            unit: "second",
            displayFormats: {
              second: "h:mm:ss a",
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
            enabled: true,
            mode: "x",
          },
        },
      },
    },
  });

  // 成交量图表设置
  const sizeCtx = document.getElementById("sizeChart").getContext("2d");
  const sizeChart = new Chart(sizeCtx, {
    type: "bar",
    data: {
      labels: labels,
      datasets: [
        {
          label: "Size per Second",
          data: sizePerSecData,
          backgroundColor: "rgba(54, 162, 235, 0.5)",
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
        x: {
          type: "time",
          time: {
            unit: "second",
            displayFormats: {
              second: "h:mm:ss a",
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
            enabled: true,
            mode: "x",
          },
        },
      },
    },
  });

  // WebSocket连接
  const ws = new WebSocket("wss://raphaelfang.com/stock/v1/ws/data");

  ws.onopen = function () {
    console.log("WebSocket connection opened");
  };

  ws.onmessage = function (event) {
    const incomingData = JSON.parse(event.data);
    console.log("Received data: ", incomingData);

    // 使用start字段作为时间标签
    const timeLabel = new Date(incomingData.start).toLocaleTimeString("en-US", { hour12: true });

    if (!labels.includes(timeLabel)) {
      labels.push(timeLabel);
      if (labels.length > maxDataPoints) {
        labels.shift();
        vwapPriceData.shift();
        sma5Data.shift();
        sizePerSecData.shift();
        pricePercentageChange.shift();
        priceColors.shift();
      }
    }

    if (incomingData.type === "per_sec_data") {
      const vwapPrice = incomingData.vwap_price_per_sec;
      const priceChange = incomingData.price_change_percentage;

      vwapPriceData.push(vwapPrice);
      pricePercentageChange.push(priceChange);
      sizePerSecData.push(incomingData.size_per_sec);

      // 根据百分比变化设置颜色
      if (priceChange > 0) {
        priceColors.push("rgba(255, 0, 0, 1)"); // 红色
      } else {
        priceColors.push("rgba(0, 128, 0, 1)"); // 绿色
      }
    } else if (incomingData.type === "MA_data") {
      sma5Data.push(incomingData.sma_5 || null);
    }

    // 更新图表
    priceChart.update();
    sizeChart.update();
  };

  ws.onerror = function (error) {
    console.error("WebSocket error: ", error);
  };

  ws.onclose = function () {
    console.log("WebSocket connection closed");
  };
});

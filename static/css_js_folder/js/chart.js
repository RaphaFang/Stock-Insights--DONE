document.addEventListener("DOMContentLoaded", function () {
  const symbols = ["2330", "0050", "00670L", "2454", "6115"];
  const charts = {}; // 存储每个symbol对应的图表实例

  symbols.forEach((symbol) => {
    const labels = [];
    const vwapPriceData = [];
    const sma5Data = [];
    const sizePerSecData = [];
    const pricePercentageChange = [];
    const priceColors = [];

    const priceCtx = document.getElementById(`priceChart_${symbol}`).getContext("2d");
    const priceChart = new Chart(priceCtx, {
      type: "line",
      data: {
        labels: labels,
        datasets: [
          {
            label: "VWAP Price per Second",
            data: vwapPriceData,
            borderColor: "rgba(255, 99, 132, 1)",
            borderWidth: 2,
            fill: false,
            spanGaps: true,
            yAxisID: "y1",
          },
          {
            label: "5 Period SMA",
            data: sma5Data,
            borderColor: "rgba(75, 192, 192, 1)",
            borderWidth: 2,
            fill: false,
            spanGaps: true,
            yAxisID: "y1",
          },
          {
            label: "Price Change (%)",
            data: pricePercentageChange,
            borderColor: function (context) {
              const index = context.dataIndex;
              return priceColors[index];
            },
            borderWidth: 2,
            fill: false,
            yAxisID: "y2",
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
            grid: {
              display: false, // Remove horizontal grid lines
            },
            ticks: {
              callback: function (value) {
                return value.toFixed(2);
              },
            },
          },
          y2: {
            type: "linear",
            position: "right",
            title: {
              display: true,
              text: "Percentage Change (%)",
            },
            grid: {
              display: false, // Remove vertical grid lines
            },
            ticks: {
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
            grid: {
              display: false, // Remove vertical grid lines
            },
          },
        },
        plugins: {
          zoom: {
            pan: {
              enabled: true,
              mode: "x",
            },
            wheel: {
              enabled: true,
              mode: "x",
            },
            drag: {
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
    });

    const sizeCtx = document.getElementById(`sizeChart_${symbol}`).getContext("2d");
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
            grid: {
              display: false, // Remove horizontal grid lines
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
            grid: {
              display: false, // Remove vertical grid lines
            },
          },
        },
        plugins: {
          zoom: {
            pan: {
              enabled: true,
              mode: "x",
            },
            wheel: {
              enabled: true,
              mode: "x",
            },
            drag: {
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
    });

    charts[symbol] = {
      priceChart,
      sizeChart,
      labels,
      vwapPriceData,
      sma5Data,
      sizePerSecData,
      pricePercentageChange,
      priceColors,
    };
  });

  // WebSocket连接
  const ws = new WebSocket("wss://raphaelfang.com/stock/v1/ws/data");

  ws.onopen = function () {
    console.log("WebSocket connection opened");
  };

  ws.onmessage = function (event) {
    const incomingData = JSON.parse(event.data);
    console.log("Received Data:", incomingData); // 调试信息，确认收到的数据

    const symbol = incomingData.symbol;

    if (!charts[symbol]) {
      console.error(`No chart found for symbol: ${symbol}`);
      return;
    }

    const chartData = charts[symbol];

    // If `yesterday_price` is available, set the middle of the y-axis
    if (incomingData.yesterday_price) {
      const yesterdayPrice = incomingData.yesterday_price;
      chartData.priceChart.options.scales.y1.min = yesterdayPrice - yesterdayPrice * 0.1;
      chartData.priceChart.options.scales.y1.max = yesterdayPrice + yesterdayPrice * 0.1;
      chartData.priceChart.options.scales.y1.ticks = {
        stepSize: yesterdayPrice * 0.02,
      };
    }

    const timeLabel = new Date(incomingData.start).toLocaleTimeString("en-US", { hour12: true });
    console.log("Time Label:", timeLabel); // 调试信息，确认时间标签

    if (chartData.labels.length > 50) {
      chartData.labels.shift();
      chartData.vwapPriceData.shift();
      chartData.sizePerSecData.shift();
      chartData.sma5Data.shift();
      chartData.pricePercentageChange.shift();
      chartData.priceColors.shift();
    }

    chartData.labels.push(timeLabel);

    if (incomingData.type === "per_sec_data") {
      chartData.vwapPriceData.push(incomingData.vwap_price_per_sec);
      chartData.pricePercentageChange.push(incomingData.price_change_percentage);
      chartData.sizePerSecData.push(incomingData.size_per_sec);

      if (incomingData.price_change_percentage > 0) {
        chartData.priceColors.push("rgba(255, 0, 0, 1)");
      } else {
        chartData.priceColors.push("rgba(0, 128, 0, 1)");
      }
    } else if (incomingData.type === "MA_data" && incomingData.MA_type === "5_MA_data") {
      chartData.sma5Data.push(incomingData.sma_5 || null);
    }

    console.log("Updating charts for symbol:", symbol); // 调试信息，确认图表更新
    chartData.priceChart.update();
    chartData.sizeChart.update();
  };

  ws.onerror = function (error) {
    console.error("WebSocket error: ", error);
  };

  ws.onclose = function () {
    console.log("WebSocket connection closed");
  };
});

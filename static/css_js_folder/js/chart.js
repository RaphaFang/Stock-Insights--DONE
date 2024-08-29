document.addEventListener("DOMContentLoaded", function () {
  const charts = {}; // 存储每个symbol对应的图表实例

  const createChartSet = (symbol) => {
    if (!charts[symbol]) {
      const chartSetContainer = document.createElement("div");
      chartSetContainer.className = "chart-set";

      const priceChartContainer = document.createElement("div");
      priceChartContainer.className = "chart-container";
      const priceCanvas = document.createElement("canvas");
      priceCanvas.id = `priceChart_${symbol}`;
      priceChartContainer.appendChild(priceCanvas);

      const sizeChartContainer = document.createElement("div");
      sizeChartContainer.className = "chart-container";
      const sizeCanvas = document.createElement("canvas");
      sizeCanvas.id = `sizeChart_${symbol}`;
      sizeChartContainer.appendChild(sizeCanvas);

      chartSetContainer.appendChild(priceChartContainer);
      chartSetContainer.appendChild(sizeChartContainer);
      document.getElementById("chartSetsContainer").appendChild(chartSetContainer);

      const priceCtx = document.getElementById(`priceChart_${symbol}`).getContext("2d");
      const priceChart = new Chart(priceCtx, {
        type: "line",
        data: {
          labels: [],
          datasets: [
            {
              label: "VWAP Price per Second",
              data: [],
              borderColor: "rgba(255, 99, 132, 1)",
              borderWidth: 2,
              fill: false,
              yAxisID: "y1",
            },
            {
              label: "5 Period SMA",
              data: [],
              borderColor: "rgba(75, 192, 192, 1)",
              borderWidth: 2,
              fill: false,
              yAxisID: "y1",
            },
            {
              label: "Price Change (%)",
              data: [],
              borderColor: [],
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
            },
            y2: {
              type: "linear",
              position: "right",
              title: {
                display: true,
                text: "Percentage Change (%)",
              },
              ticks: {
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

      const sizeCtx = document.getElementById(`sizeChart_${symbol}`).getContext("2d");
      const sizeChart = new Chart(sizeCtx, {
        type: "bar",
        data: {
          labels: [],
          datasets: [
            {
              label: "Size per Second",
              data: [],
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

      charts[symbol] = { priceChart, sizeChart };
    }
  };

  const updateChartData = (symbol, incomingData) => {
    const { priceChart, sizeChart } = charts[symbol];
    const timeLabel = new Date(incomingData.start).toLocaleTimeString("en-US", { hour12: true });

    if (incomingData.type === "per_sec_data") {
      priceChart.data.labels.push(timeLabel);
      priceChart.data.datasets[0].data.push(incomingData.vwap_price_per_sec);
      priceChart.data.datasets[2].data.push(incomingData.price_change_percentage);
      priceChart.data.datasets[2].borderColor.push(incomingData.price_change_percentage > 0 ? "rgba(255, 0, 0, 1)" : "rgba(0, 128, 0, 1)");

      sizeChart.data.labels.push(timeLabel);
      sizeChart.data.datasets[0].data.push(incomingData.size_per_sec);
    } else if (incomingData.type === "MA_data") {
      priceChart.data.datasets[1].data.push(incomingData.sma_5 || null);
    }

    // 保持数据点数量在50以内
    if (priceChart.data.labels.length > 50) {
      priceChart.data.labels.shift();
      priceChart.data.datasets.forEach((dataset) => dataset.data.shift());
      priceChart.data.datasets[2].borderColor.shift();

      sizeChart.data.labels.shift();
      sizeChart.data.datasets[0].data.shift();
    }

    priceChart.update();
    sizeChart.update();
  };

  // WebSocket连接
  const ws = new WebSocket("wss://raphaelfang.com/stock/v1/ws/data");

  ws.onopen = function () {
    console.log("WebSocket connection opened");
  };

  ws.onmessage = function (event) {
    const incomingData = JSON.parse(event.data);
    const symbol = incomingData.symbol;

    createChartSet(symbol);
    updateChartData(symbol, incomingData);
  };

  ws.onerror = function (error) {
    console.error("WebSocket error: ", error);
  };

  ws.onclose = function () {
    console.log("WebSocket connection closed");
  };
});

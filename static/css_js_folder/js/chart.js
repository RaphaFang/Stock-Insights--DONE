document.addEventListener("DOMContentLoaded", function () {
  const charts = {}; // Store chart instances by symbol

  const createChartSet = (symbol) => {
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
            yAxisID: "y1",
          },
          {
            label: "5 Period SMA",
            data: sma5Data,
            borderColor: "rgba(75, 192, 192, 1)",
            borderWidth: 2,
            fill: false,
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
          },
          y2: {
            type: "linear",
            position: "right",
            title: {
              display: true,
              text: "Percentage Change (%)",
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

    charts[symbol] = { priceChart, sizeChart, labels, vwapPriceData, sma5Data, sizePerSecData, pricePercentageChange, priceColors };
  };

  // WebSocket connection
  const ws = new WebSocket("wss://raphaelfang.com/stock/v1/ws/data");

  ws.onopen = function () {
    console.log("WebSocket connection opened");
  };

  ws.onmessage = function (event) {
    const incomingData = JSON.parse(event.data);
    const symbol = incomingData.symbol;

    if (!charts[symbol]) {
      createChartSet(symbol);
    }

    const { priceChart, sizeChart, labels, vwapPriceData, sma5Data, sizePerSecData, pricePercentageChange, priceColors } = charts[symbol];

    const timeLabel = new Date(incomingData.start).toLocaleTimeString("en-US", { hour12: true });

    labels.push(timeLabel);
    if (labels.length > 50) {
      labels.shift();
      vwapPriceData.shift();
      sizePerSecData.shift();
      sma5Data.shift();
      pricePercentageChange.shift();
      priceColors.shift();
    }

    if (incomingData.type === "per_sec_data") {
      vwapPriceData.push(incomingData.vwap_price_per_sec);
      pricePercentageChange.push(incomingData.price_change_percentage);

      if (incomingData.price_change_percentage > 0) {
        priceColors.push("rgba(255, 0, 0, 1)");
      } else {
        priceColors.push("rgba(0, 128, 0, 1)");
      }

      sizePerSecData.push(incomingData.size_per_sec);
    } else if (incomingData.type === "MA_data") {
      sma5Data.push(incomingData.sma_5 || null);
    }

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

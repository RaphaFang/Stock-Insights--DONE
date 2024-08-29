document.addEventListener("DOMContentLoaded", function () {
  const labels = [];
  const vwapPriceData = [];
  const sizePerSecData = [];
  const sma5Data = [];

  const ctx = document.getElementById("priceChart").getContext("2d");
  const stockChart = new Chart(ctx, {
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
        },
        {
          label: "5 Period SMA",
          data: sma5Data,
          borderColor: "rgba(75, 192, 192, 1)",
          borderWidth: 2,
          fill: false,
        },
      ],
    },
    options: {
      scales: {
        y: {
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
          title: {
            display: true,
            text: "Size",
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

  const ws = new WebSocket("wss://raphaelfang.com/stock/v1/ws/data");

  ws.onopen = function () {
    console.log("WebSocket connection opened");
  };

  ws.onmessage = function (event) {
    const incomingData = JSON.parse(event.data);
    console.log("Received data: ", incomingData);

    const timeLabel = new Date(incomingData.start).toLocaleTimeString("en-US", { hour12: true });
    labels.push(timeLabel);

    vwapPriceData.push(incomingData.vwap_price_per_sec);
    sizePerSecData.push(incomingData.size_per_sec);

    if (incomingData.sma_5 !== undefined) {
      sma5Data.push(incomingData.sma_5);
    }

    stockChart.update();
    sizeChart.update();
  };

  ws.onerror = function (error) {
    console.error("WebSocket error: ", error);
  };

  ws.onclose = function () {
    console.log("WebSocket connection closed");
  };
});

"use strict";

document.addEventListener("DOMContentLoaded", async function () {
  const response = await fetch("/tdt/v1/api/orders/history", {
    method: "GET",
    headers: {
      "Content-Type": "application/json",
    },
    credentials: "include",
  });
  const result = await response.json();
  console.log(response.ok);
  if (response.ok) {
    console.log("得到歷史資料");
    console.log(result);
    renderInfo(result);
  } else {
    if (response.status === 403) {
      window.location.href = "/tdt/v1/";
    } else if (response.status === 400) {
      alert("Facing error display history orders.");
    }
  }
});
function renderInfo(result) {
  const target = document.getElementById("result-display-target");
  target.innerHTML = "";
  target.innerHTML = result.map((item) => format(item)).join("");
}

function format(d) {
  return `
  <div class="result-display" >
    <div>
    <div id="att-img-div"><img class="att-img" src="${d.data.trip.attraction.image}" /></div>
    <div class="contain-inf">
      <div class="ttitle">
        <p class="sstate">Status :</p>
        <p class="sstate-c" id="thepay">PAID</p>
      </div>
      <div class="divider-div">
        <div class="divider"></div>
      </div>
      <div class="">
        <p class="sstate-2">Order code :</p>
      </div>
      <div class="thenumber-div">
        <p class="thenumber" id="thenumber">${d.data.number}</p>
      </div>
      <div class="divider-div">
        <div class="divider"></div>
      </div>
      <div class="theattr-div" id="theattr-div">
        <p class="theattr-p">Attr : ${d.data.trip.attraction.name}</p>
        <p class="theattr-p">Date : ${d.data.trip.date}</p>
        <p class="theattr-p">Time : ${d.data.trip.time}</p>
        <p class="theattr-p">Loc  : ${d.data.trip.attraction.address}</p>
      </div>
      <div class="divider-div">
        <div class="divider"></div>
      </div>
      <div class="theattr-div" id="theattr-div-2">
        <p class="theattr-p">Name : ${d.data.contact.name}</p>
        <p class="theattr-p">email: ${d.data.contact.email}</p>
        <p class="theattr-p">Phone: ${d.data.contact.phone}</p>
      </div>
      <div class="final-part">
        <p class="final-part-p">Please remember this order code for future inquiries.</p>
      </div>
    </div>
  </div>
  </div>
  `;
}

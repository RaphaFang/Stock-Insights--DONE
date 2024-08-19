"use strict";

document.addEventListener("DOMContentLoaded", async function () {
  await orderGet();
});
function getQueryParams() {
  const params = {};
  const queryString = window.location.search;
  const urlParams = new URLSearchParams(queryString);
  urlParams.forEach((value, key) => {
    params[key] = value;
  });
  return params.number;
}
const orderNum = getQueryParams();
console.log(orderNum);

async function orderGet() {
  const response = await fetch(`/tdt/v1/api/order/${orderNum}`, {
    method: "GET",
    headers: {
      "Content-Type": "application/json",
    },
  });
  const result = await response.json();
  if (response.ok) {
    console.log(result.data);
    displayThankYouPage(result.data);
    // alert("thank you for booking");
  } else {
    alert(result.message);
    console.log(result.message);
  }
}

function displayThankYouPage(d) {
  const bookInfoPic = document.getElementById("att-img-div");
  bookInfoPic.innerHTML = `
  <img class="att-img" src="${d.trip.attraction.image}" />`;

  const payState = document.getElementById("thepay");
  payState.textContent = d.status === 1 ? "已完成結帳" : "尚未付款";
  payState.className = d.status === 1 ? "sstate-c" : "sstate-type2";

  const thenumber = document.getElementById("thenumber");
  thenumber.textContent = d.number;

  let attrBlock = document.getElementById("theattr-div");
  attrBlock.innerHTML += `
    <p class="theattr-p">景點：${d.trip.attraction.name}</p>
    <p class="theattr-p">日期：${d.trip.date}</p>
    <p class="theattr-p">時間：${d.trip.time}</p>
    <p class="theattr-p">地點：${d.trip.attraction.address}</p>
    `;
  let contactBlock = document.getElementById("theattr-div-2");
  contactBlock.innerHTML += `
    <p class="theattr-p">聯絡姓名：${d.contact.name}</p>
    <p class="theattr-p">連絡信箱：${d.contact.email}</p>
    <p class="theattr-p">手機號碼：${d.contact.phone}</p>
      `;
}

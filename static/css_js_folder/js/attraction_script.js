"use strict";
let picDataAll = [];
var urlAttractionId;
let currentPic = 0;
const leftBtn = document.getElementById("left-mrt-btn");
const rightBtn = document.getElementById("right-mrt-btn");
const indicatorDivBar = document.getElementById("indicator");
let pathname = window.location.pathname;
let pathSegments = pathname.split("/");

document.addEventListener("DOMContentLoaded", async function () {
  await firstVisitPageCheck();

  backtoMain();

  spotDisplay(picDataAll);

  leftBtn.addEventListener("click", function () {
    currentPic -= 1;
    picAndDisplay(currentPic, picDataAll);
  });

  rightBtn.addEventListener("click", function () {
    currentPic += 1;
    picAndDisplay(currentPic, picDataAll);
  });

  indicatorDivBar.addEventListener("click", async function () {
    currentPic = await spotsClick(); // 等待 spotsClick 的 currentPic 所以這整個函數會是異步的
    console.log("spotsClick func., currentPic value: " + currentPic);
    picAndDisplay(Number(currentPic), picDataAll);
  });
});

async function firstVisitPageCheck() {
  urlAttractionId = Number(pathSegments[pathSegments.length - 1]);
  try {
    let response = await fetch(`/tdt/v1/api/attraction/${urlAttractionId}`);
    if (!response.ok) {
      window.location.href = "/tdt/v1/";
    } else {
      let data = await response.json();
      console.log(data["data"]);
      displayDescribe(data["data"]);
      picDataAll = data["data"]["images"];
    }
  } catch (error) {
    console.error("Fetch error: ", error);
    window.location.href = "/tdt/v1/";
  }
}
function picAndDisplay(currentPic, picDataAll) {
  const theLength = picDataAll.length;
  let theNumToDisplay = ((currentPic % theLength) + theLength) % theLength;

  currentPic = theNumToDisplay;
  console.log("picAndDisplay func. theNumToDisplay value:", theNumToDisplay);

  let describePic = document.getElementById("pic-on-left");
  describePic.style.backgroundImage = `url(${picDataAll[theNumToDisplay]})`;

  let boxElements = document.querySelectorAll(".choose-all-box");
  boxElements.forEach(function (boxElements) {
    boxElements.className = "box-1 choose-all-box";
  });
  document.getElementById(`dot-id-${theNumToDisplay}`).className = "box-2 choose-all-box";
  //   只要在每次更新全部class name時，同時也附加上choose-all-box就可以解決全部取代，後面選不上的問題
}

function spotsClick() {
  // 形式上，一定要透過 return new Promise 這方式返回 value，因為這個func.的操作都是異步的
  //   術語上叫做「封裝異步操作」
  let boxElements = document.querySelectorAll(".choose-all-box");
  return new Promise((resolve) => {
    boxElements.forEach((box) => {
      box.addEventListener("click", function (event) {
        let spotId = event.target.id;
        if (spotId) {
          let pathSegments = spotId.split("-");
          let currentPic = Number(pathSegments[pathSegments.length - 1]);
          //   這裡切開來的東西會是字串，要轉換成num
          console.log(currentPic);
          resolve(currentPic);
        }
      });
    });
  });
}

document.addEventListener("DOMContentLoaded", function () {
  const morningRadio = document.getElementById("morning");
  const afternoonRadio = document.getElementById("afternoon");
  const chargeP = document.getElementById("charge-p");

  morningRadio.addEventListener("change", function () {
    if (morningRadio.checked) {
      chargeP.textContent = "2000 NTD";
      morningRadio.checked = true;
      afternoonRadio.checked = false;
    }
  });
  afternoonRadio.addEventListener("change", function () {
    if (afternoonRadio.checked) {
      chargeP.textContent = "2500 NTD";
      morningRadio.checked = false;
      afternoonRadio.checked = true;
    }
  });
});

function displayDescribe(data) {
  let describeBlock = document.getElementById("describe-block");
  describeBlock.innerHTML += `
    <div class="the-discribe-text">${data["description"]}</div>
    <div class="location-title">Attraction address:</div>
    <div class="location" id='location-id'>${data["address"]}</div>
    <div class="transport-title">Transportation:</div>
    <div class="transport-text">${data["transport"]}</div>
    `;

  let describePic = document.getElementById("pic-on-left");
  describePic.style.backgroundImage = `url(${data["images"][0]})`;

  let describeTitle = document.getElementById("title-of-attrac");
  describeTitle.innerHTML = data["name"];
  let describeTag = document.getElementById("tag-mrt-of-attrac");
  describeTag.innerHTML = `${data["category"]} at ${data["mrt"]}`;
}

function spotDisplay(picDataAll) {
  const lenAll = picDataAll.length;
  let indicator = document.getElementById("indicator");
  for (let n = 0; n < lenAll; n++) {
    indicator.innerHTML += `
    <div class="box-1 choose-all-box" id="dot-id-${n}"></div>
    `;
  }
  document.getElementById(`dot-id-0`).className = "box-2 choose-all-box";
  //   只要在每次更新全部class name時，同時也附加上choose-all-box就可以解決全部取代，後面選不上的問題
}
// 加載日期
window.onload = function () {
  var today = new Date().toISOString().split("T")[0];
  document.getElementById("date").value = today;
};

"use strict";
// backtoMain
document.addEventListener("DOMContentLoaded", async function () {
  backtoMain();
});
function backtoMain() {
  let titleElement = document.getElementById("title");
  titleElement.addEventListener("click", function () {
    window.location.href = "/tdt/v1/";
  });
}

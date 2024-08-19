document.addEventListener("DOMContentLoaded", function () {
  function startLoading() {
    NProgress.start();
  }
  function stopLoading() {
    NProgress.done();
  }
  startLoading();
  setTimeout(() => {
    stopLoading();
  }, 3000);
});

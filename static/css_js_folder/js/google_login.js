function googleLogin() {
  const userInfo = JSON.parse(localStorage.getItem("userInfo"));
  if (!userInfo) {
    window.location.href = "https://raphaelfang.com/tdt/v1/auth/login";
  }
}

window.onload = function () {
  const urlParams = new URLSearchParams(window.location.search);
  const status = urlParams.get("status");
  const message = urlParams.get("message");

  if (status === "success") {
    alert("Login successful!");
  }
  if (status === "error") {
    alert("Login failed: " + message);
  }
};
